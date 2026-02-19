use anyhow::{Context, Result};
use bzip2::read::BzDecoder;
use clap::Parser;
use crossbeam_channel::{bounded, Sender};
use quick_xml::events::Event;
use quick_xml::Reader;
use regex::Regex;
use serde::Serialize;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input Wikipedia XML dump file (bz2 compressed)
    #[arg(short, long)]
    input: PathBuf,

    /// Output directory
    #[arg(short, long)]
    output: PathBuf,

    /// Number of worker threads (default: number of CPU cores)
    #[arg(short, long)]
    threads: Option<usize>,
}

#[derive(Debug, Clone)]
struct RawPage {
    id: String,
    title: String,
    text: String,
}

#[derive(Serialize)]
struct WikiArticle {
    id: String,
    title: String,
    text: String,
}

struct WikiExtractor {
    output_dir: PathBuf,
    current_dir_idx: usize,
    current_file: usize,
    buffer: Vec<String>,
    article_count: usize,
}

impl WikiExtractor {
    fn new(output_dir: PathBuf) -> Result<Self> {
        // Create output subdirectories (AA, BB, CC, ..., ZZ)
        for letter in b'A'..=b'Z' {
            let letter_char = letter as char;
            let dir_name = format!("{}{}", letter_char, letter_char);
            create_dir_all(output_dir.join(&dir_name))?;
        }

        Ok(Self {
            output_dir,
            current_dir_idx: 0,
            current_file: 0,
            buffer: Vec::new(),
            article_count: 0,
        })
    }

    fn get_dir_name(idx: usize) -> String {
        let idx = idx.min(25); // Cap at ZZ
        let letter = (b'A' + idx as u8) as char;
        format!("{}{}", letter, letter)
    }

    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let dir_name = Self::get_dir_name(self.current_dir_idx);
        let file_path = self
            .output_dir
            .join(&dir_name)
            .join(format!("wiki_{:02}", self.current_file));

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .context(format!("Failed to open file: {:?}", file_path))?;

        for line in &self.buffer {
            writeln!(file, "{}", line)?;
        }

        self.buffer.clear();
        self.current_file += 1;

        // Switch to next directory every 100 files
        if self.current_file >= 100 {
            self.current_file = 0;
            self.current_dir_idx += 1;
            if self.current_dir_idx < 26 {
                println!(
                    "Switching to directory {}",
                    Self::get_dir_name(self.current_dir_idx)
                );
            }
        }

        Ok(())
    }

    fn add_article(&mut self, article: WikiArticle) -> Result<()> {
        let json = serde_json::to_string(&article)?;
        self.buffer.push(json);
        self.article_count += 1;

        // Flush buffer every 100 articles
        if self.buffer.len() >= 100 {
            self.flush_buffer()?;
        }

        Ok(())
    }

    fn finish(mut self) -> Result<usize> {
        self.flush_buffer()?;
        println!("Extraction complete: {} articles", self.article_count);
        Ok(self.article_count)
    }
}

fn strip_wikitext(text: &str) -> String {
    // Basic wikitext stripping (simplified version)
    let mut result = text.to_string();

    // Remove templates: {{...}}
    let template_re = Regex::new(r"\{\{[^}]*\}\}").unwrap();
    result = template_re.replace_all(&result, "").to_string();

    // Remove file/image links: [[File:...]] or [[Image:...]]
    let file_re = Regex::new(r"\[\[(File|Image):[^\]]*\]\]").unwrap();
    result = file_re.replace_all(&result, "").to_string();

    // Convert wiki links: [[Link|Text]] -> Text or [[Link]] -> Link
    let link_re = Regex::new(r"\[\[([^|\]]+)\|([^\]]+)\]\]").unwrap();
    result = link_re.replace_all(&result, "$2").to_string();
    let link_simple_re = Regex::new(r"\[\[([^\]]+)\]\]").unwrap();
    result = link_simple_re.replace_all(&result, "$1").to_string();

    // Remove bold/italic: '''text''' or ''text''
    result = result.replace("'''", "").replace("''", "");

    // Remove HTML comments: <!-- ... -->
    let comment_re = Regex::new(r"<!--.*?-->").unwrap();
    result = comment_re.replace_all(&result, "").to_string();

    // Remove external links: [http://... text] -> text
    let ext_link_re = Regex::new(r"\[https?://[^\s\]]+ ([^\]]+)\]").unwrap();
    result = ext_link_re.replace_all(&result, "$1").to_string();
    let ext_link_simple_re = Regex::new(r"\[https?://[^\s\]]+\]").unwrap();
    result = ext_link_simple_re.replace_all(&result, "").to_string();

    // Remove heading markers: == Heading ==
    let heading_re = Regex::new(r"=+\s*([^=]+)\s*=+").unwrap();
    result = heading_re.replace_all(&result, "$1").to_string();

    // Remove references: <ref>...</ref> or <ref ... />
    let ref_re = Regex::new(r"<ref[^>]*>.*?</ref>").unwrap();
    result = ref_re.replace_all(&result, "").to_string();
    let ref_self_re = Regex::new(r"<ref[^>]*/\s*>").unwrap();
    result = ref_self_re.replace_all(&result, "").to_string();

    // Remove other HTML-like tags
    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    result = tag_re.replace_all(&result, "").to_string();

    // Clean up excess whitespace
    let whitespace_re = Regex::new(r"\s+").unwrap();
    result = whitespace_re.replace_all(&result, " ").to_string();

    result.trim().to_string()
}

fn xml_reader_thread(
    input_path: PathBuf,
    sender: Sender<Option<RawPage>>,
) -> Result<()> {
    let file = File::open(&input_path)
        .context(format!("Failed to open input file: {:?}", input_path))?;
    let decoder = BzDecoder::new(file);
    let buf_reader = BufReader::new(decoder);

    let mut reader = Reader::from_reader(buf_reader);

    let mut buf = Vec::new();
    let mut in_page = false;
    let mut in_title = false;
    let mut in_text = false;
    let mut in_id = false;
    let mut in_ns = false;

    let mut current_title = String::new();
    let mut current_text = String::new();
    let mut current_id = String::new();
    let mut current_ns = String::new();
    let mut is_redirect = false;

    println!("Processing Wikipedia dump...");

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                match e.name().as_ref() {
                    b"page" => {
                        in_page = true;
                        current_title.clear();
                        current_text.clear();
                        current_id.clear();
                        current_ns.clear();
                        is_redirect = false;
                    }
                    b"title" if in_page => in_title = true,
                    b"text" if in_page => in_text = true,
                    b"id" if in_page && current_id.is_empty() => in_id = true,
                    b"ns" if in_page => in_ns = true,
                    b"redirect" if in_page => is_redirect = true,
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                match e.name().as_ref() {
                    b"page" => {
                        // Send page to workers if it's a valid article
                        if !is_redirect && current_ns == "0" && !current_text.is_empty() {
                            let page = RawPage {
                                id: current_id.clone(),
                                title: current_title.clone(),
                                text: current_text.clone(),
                            };

                            if sender.send(Some(page)).is_err() {
                                break; // Channel closed
                            }
                        }
                        in_page = false;
                    }
                    b"title" => in_title = false,
                    b"text" => in_text = false,
                    b"id" => in_id = false,
                    b"ns" => in_ns = false,
                    _ => {}
                }
            }
            Ok(Event::Text(e)) => {
                if in_title {
                    current_title = e.unescape().unwrap_or_default().to_string();
                } else if in_text {
                    current_text = e.unescape().unwrap_or_default().to_string();
                } else if in_id {
                    current_id = e.unescape().unwrap_or_default().to_string();
                } else if in_ns {
                    current_ns = e.unescape().unwrap_or_default().to_string();
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                eprintln!("Error parsing XML at position {}: {}", reader.buffer_position(), e);
            }
            _ => {}
        }
        buf.clear();
    }

    // Send None to signal end
    drop(sender);
    Ok(())
}

fn truncate_at_char_boundary(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }

    // Find the last valid char boundary at or before max_bytes
    let mut boundary = max_bytes;
    while boundary > 0 && !s.is_char_boundary(boundary) {
        boundary -= 1;
    }

    s[..boundary].to_string()
}

fn worker_thread(
    receiver: crossbeam_channel::Receiver<Option<RawPage>>,
    sender: Sender<Option<WikiArticle>>,
) {
    while let Ok(Some(page)) = receiver.recv() {
        let stripped = strip_wikitext(&page.text);

        // Skip very short articles
        if stripped.len() < 100 {
            continue;
        }

        // Truncate at valid character boundary
        let text_truncated = truncate_at_char_boundary(&stripped, 10000);

        let article = WikiArticle {
            id: page.id,
            title: page.title,
            text: text_truncated,
        };

        if sender.send(Some(article)).is_err() {
            break; // Channel closed
        }
    }
}

fn writer_thread(
    receiver: crossbeam_channel::Receiver<Option<WikiArticle>>,
    output_dir: PathBuf,
    article_count: Arc<AtomicUsize>,
) -> Result<()> {
    let mut extractor = WikiExtractor::new(output_dir)?;

    while let Ok(Some(article)) = receiver.recv() {
        if let Err(e) = extractor.add_article(article) {
            eprintln!("Error writing article: {}", e);
        }
        let count = article_count.fetch_add(1, Ordering::Relaxed) + 1;

        // Progress reporting every 10000 articles
        if count % 10000 == 0 {
            println!("Processed {} articles...", count);
        }
    }

    extractor.finish()?;
    Ok(())
}

fn process_wikipedia_dump(
    input_path: &PathBuf,
    output_dir: &PathBuf,
    num_workers: usize,
) -> Result<()> {
    println!("Using {} worker threads for processing", num_workers);

    // Create channels
    let (raw_sender, raw_receiver) = bounded::<Option<RawPage>>(1000);
    let (article_sender, article_receiver) = bounded::<Option<WikiArticle>>(1000);

    let article_count = Arc::new(AtomicUsize::new(0));

    // Spawn reader thread
    let input_path_clone = input_path.clone();
    let reader_handle = thread::spawn(move || {
        xml_reader_thread(input_path_clone, raw_sender)
    });

    // Spawn worker threads
    let mut worker_handles = Vec::new();
    for _ in 0..num_workers {
        let rx = raw_receiver.clone();
        let tx = article_sender.clone();
        worker_handles.push(thread::spawn(move || {
            worker_thread(rx, tx);
        }));
    }

    // Drop original senders so receivers know when all are done
    drop(raw_receiver);
    drop(article_sender);

    // Spawn writer thread
    let output_dir_clone = output_dir.clone();
    let count_clone = article_count.clone();
    let writer_handle = thread::spawn(move || {
        writer_thread(article_receiver, output_dir_clone, count_clone)
    });

    // Wait for all threads
    if let Err(e) = reader_handle.join() {
        eprintln!("Reader thread panicked: {:?}", e);
    }

    for handle in worker_handles {
        if let Err(e) = handle.join() {
            eprintln!("Worker thread panicked: {:?}", e);
        }
    }

    writer_handle
        .join()
        .map_err(|e| anyhow::anyhow!("Writer thread panicked: {:?}", e))??;

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    if !args.input.exists() {
        anyhow::bail!("Input file does not exist: {:?}", args.input);
    }

    create_dir_all(&args.output)?;

    // Determine number of worker threads
    let num_workers = args.threads.unwrap_or_else(|| num_cpus::get());

    process_wikipedia_dump(&args.input, &args.output, num_workers)?;

    Ok(())
}
