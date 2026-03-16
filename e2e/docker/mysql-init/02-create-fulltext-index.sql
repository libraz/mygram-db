USE testdb;

ALTER TABLE articles ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram;
ALTER TABLE products ADD FULLTEXT INDEX ft_description (description) WITH PARSER ngram;
