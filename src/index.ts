import { DiscourseCrawler } from './crawling'
import commandLineArgs from 'command-line-args'
import { logger, LogLevel } from './logger.js'
import { CommandLineOptions } from './types/types.js'

const optionDefinitions = [
  { name: 'url', alias: 'u', type: String },
  { name: 'dbPath', alias: 'd', type: String },
  { name: 'full', alias: 'f', type: Boolean },
  { name: 'since', alias: 's', type: String },
  { name: 'verbose', alias: 'v', type: Boolean },
]

async function main() {
  logger.setLevel(LogLevel.DEBUG)

  try {
    const options = commandLineArgs(optionDefinitions) as CommandLineOptions

    if (!options.url) {
      console.log(
        `Usage: npm start -- --url=<discourse-url> [--dbPath=<db-path>] [--full] [--since=YYYY-MM-DD] [--verbose]`,
      )
      process.exit(1)
    }

    logger.setLevel(options.verbose ? LogLevel.DEBUG : LogLevel.INFO)

    const url = options.url
    const dbPath = options.dbPath || 'discourse.db'
    const fullCrawl = options.full || false
    const sinceDate = options.since ? new Date(options.since) : null

    if (sinceDate) {
      logger.info(`Crawling content since ${sinceDate.toISOString().split('T')[0]}`)
    }

    if (fullCrawl) {
      logger.info('Performing full crawl (ignoring previous crawled state)')
    } else {
      logger.info('Performing incremental crawl from timestamp last crawled')
    }

    const crawler = await DiscourseCrawler.create(url, dbPath, { fullCrawl, sinceDate })

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
