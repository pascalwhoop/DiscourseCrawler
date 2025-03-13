# Discourse Crawler

A crawler and scraper for Discourse-based forums, inspired
by IIIA-CSIC's Python [Discourse Crawler](https://github.com/IIIA-ML/DiscourseCrawler).

[![npm version](https://img.shields.io/npm/v/discourse_crawler.svg)](https://www.npmjs.com/package/discourse_crawler)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Efficient Crawling**: Systematically crawls Discourse forums to extract all content
- **Incremental Updates**: Can perform incremental crawls, only fetching content since the last crawl
- **Rate Limiting**: Configurable rate limiting to respect target server resources
- **DuckDB Storage**: Uses DuckDB for high-performance data storage and querying
- **Command-line Interface**: Simple CLI for easy usage

## Installation

```bash
# Using npm
npm install discourse-crawler

# Using yarn
yarn add discourse-crawler

# Using pnpm
pnpm add discourse-crawler
```

### Installing from source

```bash
# Clone the repository
git clone https://github.com/trozzelle/DiscourseCrawler
cd DiscourseCrawler

# Install dependencies
pnpm install

# Run project in development 
pnpm run dev -u https://community.retool.com --rate-limit 250 --verbose

# Build the project
pnpm build

# Run test suite
pnpm test
```

## Usage

### Command Line

```bash
# Basic usage
discourse-crawler --url https://forum.example.com

# With additional options
discourse-crawler --url https://forum.example.com --db-path ./my-forum.db --full --rate-limit 1000 --verbose
```

### Command Line Options

| Option         | Alias | Description                                             | Default          |
|----------------|-------|---------------------------------------------------------|------------------|
| `--url`        | `-u`  | URL of the Discourse forum to crawl (required)          | -                |
| `--db-path`    | `-d`  | Path to save the DuckDB database                        | `./discourse.db` |
| `--full`       | `-f`  | Perform a full crawl (ignore previous crawled state)    | `false`          |
| `--since`      | `-s`  | Only crawl content since this date (format: YYYY-MM-DD) | -                |
| `--rate-limit` | `-r`  | Rate limit in milliseconds between requests             | `500`            |
| `--verbose`    | `-v`  | Enable verbose logging                                  | `false`          |

### Programmatic Usage

```typescript
import { DiscourseCrawler } from 'discourse_crawler';

async function main() {
  // Create a new crawler instance
  const crawler = await DiscourseCrawler.create(
    'https://forum.example.com',
    './my-forum.db',
    {
      fullCrawl: false,
      sinceDate: new Date('2023-01-01'),
      rateLimit: 500
    }
  );

  try {
    // Start crawling
    await crawler.crawl();
  } finally {
    // Always close the connection when done
    await crawler.close();
  }
}

main().catch(console.error);
```

