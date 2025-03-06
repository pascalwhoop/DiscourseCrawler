import { DiscourseCrawler } from './crawling'
import commandLineArgs from 'command-line-args'
import { logger, LogLevel } from './logger.js'

const optionDefinitions = [
  { name: 'url', alias: 'u', type: String },
  { name: 'dbPath', alias: 'd', type: String },
]

interface CommandLineOptions {
  url: string
  dbPath: string
}

async function main() {
  logger.setLevel(LogLevel.DEBUG)

  try {
    const options = commandLineArgs(optionDefinitions) as CommandLineOptions

    if (Object.keys(options).length < 1) {
      console.log(`Usage: npm start <discourse-url> [db-path]`)
      process.exit(1)
    }

    const url = options.url
    const dbPath = options.dbPath || 'discourse.db'

    const crawler = await DiscourseCrawler.create(url, dbPath)

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

// main().catch((error) => {
//   console.error('Unhandled error:', error)
//   process.exit(1)
// })

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
