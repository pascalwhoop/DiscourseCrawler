import axios from 'axios'
import { RateLimiterMemory } from 'rate-limiter-flexible'
import { Database, Forum, Category, Page, Topic, Post } from './database.js'
import { logger } from './logger.js'

function escapeJson(json: string): string {
  return json.replace(/'/g, "''")
}

export class DiscourseCrawler {
  private db: Database
  private baseUrl: string
  private rateLimiter: RateLimiterMemory

  private constructor(baseUrl: string, db: Database, rateLimiter: RateLimiterMemory) {
    this.baseUrl = baseUrl
    this.db = db
    this.rateLimiter = rateLimiter
  }

  public static async create(
    url: string,
    dbPath: string = 'discourse.db',
  ): Promise<DiscourseCrawler> {
    const db = await Database.create(dbPath)
    const rateLimiter = new RateLimiterMemory({
      points: 3,
      duration: 1,
      blockDuration: 60,
    })

    return new DiscourseCrawler(url, db, rateLimiter)
  }

  private async limitedFetch(url: string): Promise<any> {
    try {
      await this.rateLimiter.consume('default')

      const response = await axios.get(url)
      return response.data
    } catch (error) {
      logger.error(`Error fetching ${url}: ${error}`)
      throw Error
    }
  }

  async crawl(): Promise<void> {
    const forum = await this.getForum()
    await this.crawlForum(forum)
  }

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

  private async crawlForum(forum: Forum): Promise<void> {
    logger.info(`Starting crawling forum: ${forum.url}`)

    if (!forum.categories_crawled) {
      const categoriesUrl = new URL(forum.url + '/categories.json')
      const categoryData = await this.limitedFetch(categoriesUrl.toString())
      const categoryList = categoryData.category_list

      logger.info(`Categories: ${Object.keys(categoryList.categories)}`)

      for (const category of categoryList.categories) {
        await this.db.createCategory({
          category_id: category.id,
          forum_id: forum.id,
          topic_url: category.topic_url,
          json: escapeJson(JSON.stringify(category)),
          pages_crawled: false,
        })
      }

      await this.db.updateForum(forum.id, { categories_crawled: true })
      logger.info('Categories committed to database')
    } else {
      logger.info(`Skipping obtaining categories`)
    }

    const categories = await this.db.findCategoriesByForumId(forum.id)

    for (const category of categories) {
      await this.crawlCategory(category)
    }

    await this.crawlTopics(forum)

    logger.info(`Completed crawling forum ${forum.id}`)
  }

  private async crawlCategory(category: Category): Promise<void> {
    logger.info(`Crawling category ${category.category_id}`)

    if (!category.pages_crawled) {
      let lastPage = await this.db.getLastPageByCategory(category.id!)

      while (lastPage === null || lastPage.more_topics_url !== null) {
        let url: string
        let nextPageId: number

        if (lastPage === null) {
          const urlObj = new URL(`/c/${category.category_id}.json`, this.baseUrl)
          urlObj.searchParams.set('page', '0')
          url = urlObj.toString()
          nextPageId = 0
        } else {
          nextPageId = lastPage.page_id + 1
          const moreUrl = lastPage.more_topics_url!.replace('?', '.json?')
          const urlObj = new URL(moreUrl, this.baseUrl)
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
              topic_json: null,
              posts_crawled: false,
            })
          }
        }
        logger.info(`Page ${nextPageId} and its topics committed to db`)
        lastPage = page
      }

      await this.db.updateCategory(category.id!, { pages_crawled: true })
      logger.info(`Finished crawling category ${category.category_id}`)
    } else {
      logger.info(`Category ${category.category_id} has already been crawled.`)
    }
  }

  private async crawlTopics(forum: Forum): Promise<void> {
    const categories = await this.db.findCategoriesByForumId(forum.id)

    for (const category of categories) {
      const topics = await this.db.getTopicsByCategoryId(category.id)
      for (const topic of topics) {
        await this.crawlTopic(topic)
      }
    }
  }

  private async crawlTopic(topic: Topic): Promise<void> {
    logger.info(`Crawling topic ${topic.topic_id}`)

    if (!topic.posts_crawled) {
      try {
        const result = await this.db.connection.runAndReadAll(`
            SELECT c.*
            FROM category c
                     JOIN topic t ON c.id = t.category_id
            WHERE t.id = ${topic.id}
        `)

        const category = result.getRows()[0] as Category
        const baseUrl = new URL(this.baseUrl)
        baseUrl.pathname = `/t/${topic.topic_id}.json`
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

        await this.db.updateTopic(topic.id!, { posts_crawled: true })
      } catch (error) {
        logger.error(`Exception in topic ${topic.topic_id}: ${error}`)
      }

      logger.info(`Finished crawling topic ${topic.topic_id}`)
    } else {
      logger.info(`Topic ${topic.topic_id} has already been crawled`)
    }
  }

  private async createPosts(topic: Topic, jsonPosts: any): Promise<number> {
    const posts = jsonPosts.post_stream.posts_crawled

    for (const post of posts) {
      const existingPost = await this.db.findPostByPostIdAndTopicId(post.id, topic.id!)

      if (!existingPost) {
        await this.db.createPost({
          post_id: post.id,
          topic_id: topic.id,
          json: escapeJson(JSON.stringify(post)),
        })
      }
    }

    return posts.length
  }

  async close(): Promise<void> {
    await this.db.close()
  }
}
