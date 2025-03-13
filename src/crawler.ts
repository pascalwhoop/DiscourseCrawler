import axios from 'axios'
import { RateLimiterMemory } from 'rate-limiter-flexible'

import { Database } from './database.js'
import { logger } from './logger.js'
import { Category, CrawlerOptions, Forum, Topic } from './types/types.js'
import { escapeJson } from './utils/helpers.js'

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
    this.baseUrl = baseUrl
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

      // Adds rate limit delay if provided or defaults to 500ms
      const rateLimit = this.options?.rateLimit || 500
      await new Promise((resolve) => setTimeout(resolve, rateLimit))

      const response = await axios.get(url)
      return response.data
    } catch (error) {
      if (error.remainingPoints !== undefined) {
        const waitTime = error.msBeforeNext || 60000
        logger.warn(`Rate limit reached. Waiting ${waitTime / 1000} seconds before next request.`)

        await new Promise((resolve) => setTimeout(resolve, waitTime))

        if (retries > 0) {
          logger.info(`Retrying request to ${url}.`)
          return this.limitedFetch(url, retries - 1)
        }
      }
      logger.error(`Error fetching ${url}: ${error}`)
      throw Error
    }
  }

  /**
   * Main crawling method that orchestrates the entire crawling process.
   */
  async crawl(): Promise<void> {
    const forum = await this.getForum()

    // If fullCrawl is set, reset crawled state for all Forum objects
    if (this.options?.fullCrawl) await this.resetCrawledState(forum)

    await this.crawlForum(forum)
  }

  /**
   * Resets crawled state for all Forum objects
   * @param {Forum} forum - Forum data to reset
   * @private
   */
  private async resetCrawledState(forum: Forum): Promise<void> {
    logger.info(`Resetting crawled state for forum ${forum.id}`)

    await this.db.updateForum(forum.id!, { categories_crawled: false })

    const categories = await this.db.findCategoriesByForumId(forum.id!)

    for (const category of categories) {
      await this.db.updateCategory(category.id!, { pages_crawled: false })
    }

    // Simpler to run directly here absent adding new method
    // Resets posts_crawled and last_crawled_at for topics
    // for all categories at once
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

    logger.info('Crawled state reset.')
  }

  /**
   * Gets or creates a forum record in the database.
   * @returns {Promise<Forum>} The forum record
   * @private
   */
  private async getForum(): Promise<Forum> {
    let forum = await this.db.findForum(this.baseUrl)

    if (!forum) {
      forum = await this.db.createForum({
        url: this.baseUrl,
        categories_crawled: false,
      })
    }
    return forum
  }

  /**
   * Crawls a forum to extract categories, topics, and posts.
   * @param {Forum} forum - The forum to crawl
   * @private
   */
  private async crawlForum(forum: Forum): Promise<void> {
    logger.info(`Starting crawling forum: ${forum.url}`)

    // If we've never crawled before, get category data and create categories
    if (!forum.categories_crawled) {
      const categoriesUrl = new URL(forum.url + '/categories.json')
      const categoryData = await this.limitedFetch(categoriesUrl.toString())
      const categoryList = categoryData.category_list

      logger.info(`Categories: ${Object.keys(categoryList.categories)}`)

      for (const category of categoryList.categories) {
        await this.db.createCategory({
          category_id: category.id,
          forum_id: forum.id!,
          topic_url: category.topic_url,
          json: escapeJson(JSON.stringify(category)),
          pages_crawled: false,
        })
      }

      await this.db.updateForum(forum.id!, { categories_crawled: true })
      logger.info('Categories committed to database')
    } else {
      logger.info(`Skipping obtaining categories`)
    }

    // Retrieve all created categories and crawl them
    const categories = await this.db.findCategoriesByForumId(forum.id!)

    for (const category of categories) {
      await this.crawlCategory(category)
    }

    // Then crawl all topics
    await this.crawlTopics(forum)

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
          const urlObj = new URL(`/c/${category.category_id}.json`, this.baseUrl)
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

    // for (const category of categories) {
    //   const topics = await this.db.getTopicsByCategoryId(category.id)
    //   for (const topic of topics) {
    //     if (topic.posts_crawled && !this.options?.fullCrawl) {
    //       if (this.options?.sinceDate && topic.last_crawled_at) {
    //         const lastCrawled = new Date(topic.last_crawled_at)
    //         if (lastCrawled >= this.options.sinceDate) {
    //           logger.info(
    //             `Skipping topic ${topic.topic_id}. Already crawled since ${this.options.sinceDate}`,
    //           )
    //           continue
    //         }
    //       }
    //     }
    //     await this.crawlTopic(topic)
    //   }
    // }
  }

  /**
   * Crawls a specific topic to extract its posts.
   * @param {Topic} topic - The topic to crawl
   * @private
   */
  private async crawlTopic(topic: Topic): Promise<void> {
    logger.info(`Crawling topic ${topic.topic_id}`)

    if (!topic.posts_crawled) {
      try {
        const baseUrl = new URL(this.baseUrl)
        baseUrl.pathname = `/t/${topic.topic_id}.json`

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

          const urlObj = baseUrl
          urlObj.pathname = `/t/${topic.topic_id}/posts.json`
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
