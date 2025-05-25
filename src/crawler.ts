import axios from 'axios'
import { RateLimiterMemory } from 'rate-limiter-flexible'
import { URL } from 'url'

import { Database } from './database.js'
import { logger } from './logger.js'
import { Category, CrawlerOptions, Forum, Topic } from './types/types.js'
import { escapeJson } from './utils/helpers.js'


function updatePathname(url: URL, suffix: string): URL {
  const newPathname = url.pathname + suffix
  url.pathname = newPathname
  return url
}

/**
 * DiscourseCrawler class for crawling a Discourse forum instance
 */
export class DiscourseCrawler {
  private db: Database
  private baseUrl: string
  private rateLimiter: RateLimiterMemory
  private options: CrawlerOptions

  private constructor(
    baseUrl: string,
    db: Database,
    rateLimiter: RateLimiterMemory,
    options: CrawlerOptions = {},
  ) {
    const parsedUrl = new URL(baseUrl)
    if (parsedUrl.pathname && parsedUrl.pathname !== '/' && !parsedUrl.pathname.endsWith('/')) {
      parsedUrl.pathname += '/'
    }
    this.baseUrl = parsedUrl.toString()
    this.db = db
    this.rateLimiter = rateLimiter
    this.options = options
  }

  /**
   * Creates a new DiscourseCrawler instance with database and rate limiter.
   * @param {string} url - Base URL of the Discourse forum
   * @param {string} dbPath - Path to the database file (defaults to './discourse.db')
   * @param {CrawlerOptions} options - Additional options to control crawler behavior
   * @returns {Promise<DiscourseCrawler>} A configured DiscourseCrawler instance
   */
  public static async create(
    url: string,
    dbPath: string = './discourse.db',
    options: CrawlerOptions = {},
  ): Promise<DiscourseCrawler> {
    const db = await Database.create(dbPath)
    const rateLimiter = new RateLimiterMemory({
      points: options?.rateLimit ? Math.round(1000 / options?.rateLimit) : 3,
      duration: 1,
      blockDuration: 60,
    })

    return new DiscourseCrawler(url, db, rateLimiter, options)
  }

  /**
   * Performs a rate-limited HTTP request with retry logic.
   * @param {string} url - URL to fetch
   * @param {number} retries - Number of retries on failure (default: 3)
   * @returns {Promise<any>} Response data
   * @private
   */
  private async limitedFetch(url: string, retries = 3): Promise<any> {
    try {
      await this.rateLimiter.consume('default')
      const rateLimit = this.options?.rateLimit || 500
      await new Promise((resolve) => setTimeout(resolve, rateLimit))
      const response = await axios.get(url)
      return response.data
    } catch (error: any) {
      if (error.isAxiosError && error.response) {
        logger.error(
          `HTTP error fetching ${url}: ${error.response.status} ${error.response.statusText}\nResponse data: ${JSON.stringify(error.response.data)}`,
        )
      }
      if (error.remainingPoints !== undefined) {
        const waitTime = error.msBeforeNext || 60000
        logger.warn(`Rate limit reached for ${url}. Waiting ${waitTime / 1000} seconds before next request.`)
        await new Promise((resolve) => setTimeout(resolve, waitTime))
        if (retries > 0) {
          logger.info(`Retrying request to ${url} (${retries} retries left).`)
          return this.limitedFetch(url, retries - 1)
        }
        const newError = new Error(`Rate limit retries exhausted for ${url}. Original error: ${error.message}`)
        throw newError;
      }
      const detailedError = new Error(`Failed to fetch ${url} after multiple retries. Original error: ${error.message}`)
      logger.error(`${detailedError.message}\nStack: ${error.stack}`)
      throw detailedError
    }
  }

  /**
   * Main crawling method that orchestrates the entire crawling process.
   */
  async crawl(): Promise<void> {
    let forum: Forum | null = null;
    try {
      forum = await this.getForum()
    } catch (error: any) {
      logger.error(`Critical error: Could not get or create forum entry for ${this.baseUrl}. Error: ${error.message}\nStack: ${error.stack}`)
      throw new Error(`Could not initialize forum ${this.baseUrl}: ${error.message}`);
    }

    if (this.options?.fullCrawl && forum) {
      try {
        await this.resetCrawledState(forum!)
      } catch (error: any) {
        logger.error(`Error resetting crawled state for forum ${forum.id}. Error: ${error.message}\nStack: ${error.stack}`)
      }
    }
    if (forum) {
        try {
            await this.crawlForum(forum!)
        } catch (error: any) {
            logger.error(`An error occurred during crawlForum for ${forum.url}. Error: ${error.message}\nStack: ${error.stack}`);
        }
    } else {
        logger.error(`Forum object is null, cannot proceed with crawlForum.`);
    }
  }

  /**
   * Resets crawled state for all Forum objects
   * @param {Forum} forum - Forum data to reset
   * @private
   */
  private async resetCrawledState(forum: Forum): Promise<void> {
    logger.info(`Resetting crawled state for forum ${forum.id}`)
    try {
      await this.db.updateForum(forum.id!, { categories_crawled: false })
      const categories = await this.db.findCategoriesByForumId(forum.id!)
      for (const category of categories) {
        await this.db.updateCategory(category.id!, { pages_crawled: false })
      }
      await this.db.query(
        `
            UPDATE topic t
            SET posts_crawled   = FALSE,
                last_crawled_at = NULL
            FROM category c
            WHERE t.category_id = c.id
              AND c.forum_id = ?
        `,
        [forum.id],
      )
      logger.info('Crawled state reset complete.')
    } catch (error: any) {
      logger.error(`Failed to reset crawled state for forum ${forum.id}. Error: ${error.message}\nStack: ${error.stack}`)
      throw new Error(`Failed to reset crawled state for forum ${forum.id}: ${error.message}`);
    }
  }

  /**
   * Gets or creates a forum record in the database.
   * @returns {Promise<Forum>} The forum record
   * @private
   */
  private async getForum(): Promise<Forum> {
    try {
      let forum = await this.db.findForum(this.baseUrl)
      if (!forum) {
        logger.info(`Forum not found for ${this.baseUrl}, creating new entry.`)
        forum = await this.db.createForum({
          url: this.baseUrl,
          categories_crawled: false,
        })
      }
      return forum
    } catch (error: any) {
      logger.error(`Database error in getForum for ${this.baseUrl}. Error: ${error.message}\nStack: ${error.stack}`)
      throw new Error(`Failed to get or create forum ${this.baseUrl} in database: ${error.message}`);
    }
  }

  /**
   * Crawls a forum to extract categories, topics, and posts.
   * @param {Forum} forum - The forum to crawl
   * @private
   */
  private async crawlForum(forum: Forum): Promise<void> {
    logger.info(`Starting crawling forum: ${forum.url}`)

    if (!forum.categories_crawled || this.options?.fullCrawl) {
      try {
        const categoriesUrl = new URL('categories.json', forum.url)
        logger.info(`Fetching categories from ${categoriesUrl.toString()}`)
        const categoryData = await this.limitedFetch(categoriesUrl.toString())

        if (!categoryData || !categoryData.category_list || !Array.isArray(categoryData.category_list.categories)) {
          logger.error(`Malformed category data received from ${categoriesUrl.toString()}. Data: ${JSON.stringify(categoryData)}`)
          throw new Error(`Malformed category data from ${categoriesUrl.toString()}`);
        }
        const categoryList = categoryData.category_list
        logger.info(`Found ${categoryList.categories.length} categories for forum ${forum.url}.`)

        for (const category of categoryList.categories) {
          try {
            await this.db.createCategory({
              category_id: category.id,
              forum_id: forum.id!,
              topic_url: category.topic_url,
              json: escapeJson(JSON.stringify(category)),
              pages_crawled: false,
            })
          } catch (error: any) {
            logger.error(
              `Failed to create category ID ${category.id} (${category.slug}) in database for forum ${forum.url}. Error: ${error.message}\nStack: ${error.stack}`,
            )
          }
        }
        await this.db.updateForum(forum.id!, { categories_crawled: true })
        logger.info('Categories committed to database and forum marked as categories_crawled.')
      } catch (error: any) {
        logger.error(
          `Failed to fetch or process categories for forum ${forum.url}. Error: ${error.message}\nStack: ${error.stack}`,
        )
        if (!forum.categories_crawled) {
            throw new Error(`Critical: Could not fetch initial categories for ${forum.url}. ${error.message}`);
        }
      }
    } else {
      logger.info(`Skipping fetching categories for forum ${forum.url} as they are already marked crawled.`)
    }

    const categories = await this.db.findCategoriesByForumId(forum.id!)
    logger.info(`Proceeding to crawl ${categories.length} categories found in DB for forum ${forum.url}.`)

    for (const category of categories) {
      try {
        await this.crawlCategory(category)
      } catch (error: any) {
        logger.error(
          `Failed to crawl category ID ${category.category_id} for forum ${forum.url}. Error: ${error.message}\nStack: ${error.stack}`,
        )
      }
    }

    logger.info(`Finished crawling categories. Proceeding to crawl topics for forum ${forum.url}.`)
    try {
      await this.crawlTopics(forum)
    } catch (error: any) {
        logger.error(
          `An error occurred during crawlTopics for forum ${forum.url}. Error: ${error.message}\nStack: ${error.stack}`,
        );
    }

    logger.info(`Completed crawling forum ${forum.id}`)
  }

  /**
   * Crawls a category to extract its pages and topics.
   * @param {Category} category - The category to crawl
   * @private
   */
  private async crawlCategory(category: Category): Promise<void> {
    logger.info(`Crawling category ${category.category_id}`)

    // Either the latest last_crawled_at timestamp or sinceDate, if passed
    const sinceDate =
      this.options?.sinceDate ||
      (!this.options?.fullCrawl && (await this.db.getLatestTopicTimestamp(category.forum_id)))

    // Run full crawl if first time or if fullCrawl is set
    if (!category.pages_crawled || this.options?.fullCrawl) {
      let lastPage = await this.db.getLastPageByCategory(category.id!)

      // Paginates through a Category's pages
      while (lastPage === null || lastPage.more_topics_url !== null) {
        let url: string
        let nextPageId: number

        if (lastPage === null) {
          const urlObj = new URL(`c/${category.category_id}.json`, this.baseUrl)
          urlObj.searchParams.set('page', '0')

          // If there's existing topic data, we crawl topics created after
          // the latest timestamp in the topics table
          if (sinceDate) {
            const formattedDate = sinceDate.toISOString().split('T')[0]
            urlObj.searchParams.set('after', formattedDate)
          }

          url = urlObj.toString()
          nextPageId = 0
        } else {
          nextPageId = lastPage.page_id + 1
          const moreUrl = lastPage.more_topics_url!.replace('?', '.json?')
          const urlObj = new URL(moreUrl, this.baseUrl)

          if (sinceDate && !urlObj.searchParams.has('after')) {
            const formattedDate = sinceDate.toISOString().split('T')[0]
            urlObj.searchParams.set('after', formattedDate)
          }
          url = urlObj.toString()
        }

        logger.info(`Crawling page ${nextPageId}`)
        logger.debug(`URL: ${url}`)

        const jsonPage = await this.limitedFetch(url)
        const topicList = jsonPage.topic_list
        const moreTopicsUrl = topicList.more_topics_url || null

        const page = await this.db.createPage({
          category_id: category.id!,
          page_id: nextPageId,
          more_topics_url: moreTopicsUrl,
          json: escapeJson(JSON.stringify(jsonPage)),
        })

        for (const topic of topicList.topics) {
          const existingTopic = await this.db.findTopicByCategoryIdAndTopicId(
            category.id!,
            topic.id,
          )

          if (!existingTopic) {
            await this.db.createTopic({
              topic_id: topic.id,
              category_id: category.id!,
              page_excerpt_json: escapeJson(JSON.stringify(topic)),
              topic_json: '',
              posts_crawled: false,
            })
          }
        }
        logger.info(`Page ${nextPageId} and its topics committed to db`)
        lastPage = page

        if (!moreTopicsUrl) {
          break
        }
      }

      await this.db.updateCategory(category.id!, { pages_crawled: true })
      logger.info(`Finished crawling category ${category.category_id}`)
    } else {
      logger.info(`Category ${category.category_id} has already been crawled.`)
    }
  }

  /**
   * Crawls all topics from all categories in a forum.
   * @param {Forum} forum - The forum containing the topics
   * @private
   */
  private async crawlTopics(forum: Forum): Promise<void> {
    const categories = await this.db.findCategoriesByForumId(forum.id!)

    for (const category of categories) {
      const topics = await this.db.getTopicsByCategoryId(category.id!)
      for (const topic of topics) {
        // Skip topics that have been crawled unless we're doing a full crawl
        // or if the topic was crawled before our sinceDate
        if (topic.posts_crawled && !this.options?.fullCrawl) {
          if (this.options?.sinceDate && topic.last_crawled_at) {
            const lastCrawled = new Date(topic.last_crawled_at)
            if (lastCrawled >= this.options.sinceDate) {
              logger.info(
                `Skipping topic ${topic.topic_id} - already crawled after ${this.options.sinceDate}`,
              )
              continue
            }
          } else {
            logger.info(`Skipping topic ${topic.topic_id} - already crawled`)
            continue
          }
        }

        await this.crawlTopic(topic)
      }
    }
  }

  /**
   * Crawls a specific topic to extract its posts.
   * @param {Topic} topic - The topic to crawl
   * @private
   */
  private async crawlTopic(topic: Topic): Promise<void> {
    logger.info(`Crawling topic ${topic.topic_id}`)

    logger.info(this.baseUrl)

    if (!topic.posts_crawled) {
      try {
        const baseUrl = new URL(this.baseUrl)
        updatePathname(baseUrl, `/t/${topic.topic_id}.json`)

        let lastPostNumber = null
        if (topic.posts_crawled && !this.options?.fullCrawl) {
          lastPostNumber = await this.db.getTopicLastPostNumber(topic.id!)
          if (lastPostNumber) {
            baseUrl.searchParams.set('posts_after', lastPostNumber.toString())
            logger.info(`Getting posts after post number ${lastPostNumber}`)
          }
        }

        const url = baseUrl.toString()

        const jsonTopic = await this.limitedFetch(url)

        await this.db.updateTopic(topic.id!, {
          topic_json: escapeJson(JSON.stringify(jsonTopic)),
        })

        let nPosts = await this.createPosts(topic, jsonTopic)

        let remainingPosts = jsonTopic.post_stream.stream.slice(nPosts)

        while (remainingPosts.length > 0) {
          const nextPosts = remainingPosts.slice(0, 20)

          const urlObj = new URL(this.baseUrl)
          updatePathname(urlObj, `/t/${topic.topic_id}/posts.json`)
          urlObj.searchParams.set('post_ids[]', nextPosts)
          urlObj.searchParams.set('include_suggested', 'true')

          const url = urlObj.toString()

          logger.debug(`URL: ${url}`)

          const jsonPosts = await this.limitedFetch(url)

          nPosts = await this.createPosts(topic, jsonPosts)
          remainingPosts = remainingPosts.slice(nPosts)
        }

        await this.db.updateTopicLastCrawled(topic.id!)

        // Mark as crawled if this is the first time
        if (!topic.posts_crawled) await this.db.updateTopic(topic.id!, { posts_crawled: true })
      } catch (error) {
        logger.error(`Exception in topic ${topic.topic_id}: ${error}`)
      }

      logger.info(`Finished crawling topic ${topic.topic_id}`)
    } else {
      logger.info(`Topic ${topic.topic_id} has already been crawled`)
    }
  }

  /**
   * Creates post records from JSON data.
   * @param {Topic} topic - The topic containing the posts
   * @param {any} jsonPosts - JSON data containing posts
   * @returns {Promise<number>} Number of posts created
   * @private
   */
  private async createPosts(topic: Topic, jsonPosts: any): Promise<number> {
    const posts = jsonPosts.post_stream.posts
    let updatedCount = 0

    for (const post of posts) {
      const existingPost = await this.db.findPostByPostIdAndTopicId(post.id, topic.id!)
      const jsonPost = escapeJson(JSON.stringify(post))

      if (!existingPost) {
        await this.db.createPost({
          post_id: post.id,
          topic_id: topic.id!,
          json: jsonPost,
        })
        updatedCount++
      } else if (!this.options?.fullCrawl) {
        try {
          const wasUpdated = await this.db.updatePostIfEdited(existingPost.id!, jsonPost)
          if (wasUpdated) {
            logger.info(`New version of post ${post.id} found in topic ${topic.id}`)
            updatedCount++
          }
        } catch (error) {
          logger.error(`Error updating post ${post.id}: ${error}`)
        }
      }
    }

    logger.debug(`${updatedCount} posts inserted or updated out of ${posts.length} posts inserted`)
    return posts.length
  }

  /**
   * Closes database connection
   */
  async close(): Promise<void> {
    await this.db.close()
  }
}
