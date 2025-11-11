-- MygramDB Sample Database Initialization Script
-- This script creates sample tables for testing MygramDB

USE mydb;

-- Sample articles table
CREATE TABLE IF NOT EXISTS articles (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    status INT NOT NULL DEFAULT 1,
    category VARCHAR(50),
    enabled TINYINT NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_status (status),
    KEY idx_category (category),
    KEY idx_enabled (enabled),
    KEY idx_created_at (created_at),
    KEY idx_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert sample data
INSERT INTO articles (title, content, status, category, enabled) VALUES
    ('Welcome to MygramDB', 'MygramDB is a high-performance in-memory full-text search engine with MySQL replication support.', 1, 'announcement', 1),
    ('Getting Started', 'This guide will help you get started with MygramDB. First, configure your MySQL connection.', 1, 'tutorial', 1),
    ('日本語サンプル', 'これは日本語のサンプル記事です。MygramDBは日本語の全文検索をサポートしています。', 1, 'sample', 1),
    ('Performance Tips', 'Optimize your MygramDB performance by tuning n-gram size and memory settings.', 1, 'tips', 1),
    ('Advanced Configuration', 'Learn about advanced configuration options including filters and bitmap indexes.', 2, 'advanced', 1);

-- Show created tables
SHOW TABLES;

-- Display sample data
SELECT id, title, status, category, enabled, created_at FROM articles;
