export interface Forum {
  id?: number
  url: string
  categories_crawled: boolean
}

export interface User {
  id?: number
  forum_id: number
  json: string
}

export interface Category {
  id?: number
  category_id: number
  forum_id: number
  topic_url: string
  json: string
  pages_crawled: boolean
}

export interface Page {
  id?: number
  page_id: number
  category_id: number
  more_topics_url: string | null
  json: string
}

export interface Topic {
  id?: number
  topic_id: number
  category_id: number
  page_excerpt_json: string
  topic_json: string
  posts_crawled: boolean
}

export interface Post {
  id?: number
  post_id: number
  topic_id: number
  json: string
}

export interface CrawlerOptions {
  fullCrawl?: boolean
  sinceDate?: Date | null
  rateLimit?: number
}

export interface CommandLineOptions {
  url: string
  dbPath: string
  full?: boolean
  since?: string
  rateLimit?: number
  verbose?: boolean
}
