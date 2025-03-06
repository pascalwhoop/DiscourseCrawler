import { DiscourseCrawler } from './crawling'
import commandLineArgs from 'command-line-args'
import { logger } from './logger.js'

const optionDefinitions = [
  { name: 'url', alias: 'u', type: String },
  { name: 'dbPath', alias: 'db', type: String },
]

interface CommandLineOptions {
  url: string
  dbPath: string
}

async function main() {
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
}

main().catch((error) => {
  console.error('Unhandled error:', error)
  process.exit(1)
})
