#!/usr/bin/env node

import commandLineArgs from 'command-line-args'

import { DiscourseCrawler } from './crawling.js'
import { logger, LogLevel } from './logger.js'
import { CommandLineOptions, CrawlerOptions } from './types/types.js'

const optionDefinitions = [
  { name: 'url', alias: 'u', type: String },
  { name: 'db-path', alias: 'd', type: String },
  { name: 'full', alias: 'f', type: Boolean },
  { name: 'since', alias: 's', type: String },
  { name: 'rate-limit', alias: 'r', type: Number },
  { name: 'verbose', alias: 'v', type: Boolean },
]

async function main() {
  logger.setLevel(LogLevel.DEBUG)

  try {
    const options = commandLineArgs(optionDefinitions, { camelCase: true }) as CommandLineOptions

    if (!options.url) {
      console.log(
        `Usage: discourse-crawler --url <discourse-url> [--db-path=<db-path>] [--full] [--since=YYYY-MM-DD] [--rate-limit 500] [--verbose]`,
      )
      process.exit(1)
    }

    logger.setLevel(options.verbose ? LogLevel.DEBUG : LogLevel.INFO)

    if (options?.verbose) logger.debug('Verbose logging enabled.')

    const url = options.url
    const dbPath = options.dbPath || './discourse.db'
    const fullCrawl = options.full || false
    const sinceDate = options.since ? new Date(options.since) : null
    const rateLimit = options.rateLimit || 500

    if (sinceDate) {
      logger.info(`Crawling content since ${sinceDate.toISOString().split('T')[0]}`)
    }

    if (rateLimit) {
      logger.info(`Rate limit provided and set to ${rateLimit} ms`)
    }

    if (fullCrawl) {
      logger.info('Performing full crawl (ignoring previous crawled state)')
    } else {
      logger.info('Performing incremental crawl from timestamp last crawled')
    }

    const crawler = await DiscourseCrawler.create(url, dbPath, {
      fullCrawl,
      sinceDate,
      rateLimit,
    } as CrawlerOptions)

    try {
      await crawler.crawl()
    } catch (error) {
      logger.error(`Crawling failed: ${error}`)
    } finally {
      await crawler.close()
    }
  } catch (error) {
    logger.error(`Error in main: ${error}`)
  }
}

// Add error handling for unhandled rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason)
  process.exit(1)
})

// Call main and handle any errors
main().catch((error) => {
  console.error('Uncaught error in main:')
  console.error(error)
  process.exit(1)
})
