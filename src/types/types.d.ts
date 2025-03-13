/**
 * Represents a Discourse forum instance
 */
export interface Forum {
  /** Database ID (auto-generated) */
  id?: number
  /** URL of the Discourse forum */
  url: string
  /** Whether all categories have been crawled */
  categories_crawled: boolean
}

/**
 * Represents a Discourse user
 */
export interface User {
  /** Database ID (auto-generated) */
  id?: number
  /** ID of the forum this user belongs to */
  forum_id: number
  /** JSON string containing user data */
  json: string
}

/**
 * Represents a Discourse category
 */
export interface Category {
  /** Database ID (auto-generated) */
  id?: number
  /** Discourse category ID */
  category_id: number
  /** ID of the forum this category belongs to */
  forum_id: number
  /** URL to the category's topic */
  topic_url: string
  /** JSON string containing category data */
  json: string
  /** Whether all pages in this category have been crawled */
  pages_crawled: boolean
}

/**
 * Represents a page of topics in a Discourse category
 */
export interface Page {
  /** Database ID (auto-generated) */
  id?: number
  /** Page number within the category */
  page_id: number
  /** ID of the category this page belongs to */
  category_id: number
  /** URL to fetch more topics, or null if no more topics */
  more_topics_url: string | null
  /** JSON string containing page data */
  json: string
}

/**
 * Represents a Discourse topic
 */
export interface Topic {
  /** Database ID (auto-generated) */
  id?: number
  /** Discourse topic ID */
  topic_id: number
  /** ID of the category this topic belongs to */
  category_id: number
  /** JSON string containing topic excerpt from category page */
  page_excerpt_json: string
  /** JSON string containing full topic data */
  topic_json: string
  /** Whether all posts in this topic have been crawled */
  posts_crawled: boolean
  /** When this topic was last crawled */
  last_crawled_at?: Date
}

/**
 * Represents a Discourse post
 */
export interface Post {
  /** Database ID (auto-generated) */
  id?: number
  /** Discourse post ID */
  post_id: number
  /** ID of the topic this post belongs to */
  topic_id: number
  /** JSON string containing post data */
  json: string
}

/**
 * Options for controlling crawler behavior
 */
export interface CrawlerOptions {
  /** Whether to perform a full crawl instead of incremental */
  fullCrawl?: boolean
  /** Only crawl content since this date */
  sinceDate?: Date | null
  /** Rate limit in milliseconds between requests */
  rateLimit?: number
}

/**
 * Command line options for the crawler
 */
export interface CommandLineOptions {
  /** URL of the Discourse forum to crawl */
  url: string
  /** Path to the database file */
  dbPath: string
  /** Whether to perform a full crawl */
  full?: boolean
  /** Crawl content since this date (YYYY-MM-DD) */
  since?: string
  /** Rate limit in milliseconds between requests */
  rateLimit?: number
  /** Enable verbose logging */
  verbose?: boolean
}
