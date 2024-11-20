package postgres

import (
	"GoNews/pkg/storage"
	"context"
	"log"
)

type TaskStorage struct {
	db Client
}

func (s *TaskStorage) Posts() ([]storage.Post, error) {
	rows, err := s.db.Query(context.Background(), `
	SELECT 
		posts.id AS post_id, 
    	authors.name AS author_name,
		authors.id AS author_id, 
    	posts.title, 
    	posts.content, 
    	posts.created_at 
	FROM 
    	posts 
	JOIN 
    	authors ON posts.author_id = authors.id
	`,
	)
	if err != nil {
		return nil, err
	}

	var posts []storage.Post
	for rows.Next() {
		var post storage.Post
		err = rows.Scan(&post.ID, &post.AuthorName, &post.AuthorID, &post.Title, &post.Content, &post.CreatedAt)
		if err != nil {
			return nil, err
		}
		posts = append(posts, post)

	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return posts, nil
}

func (s *TaskStorage) AddPost(post storage.Post) error {
	_, err := s.db.Exec(context.Background(), `
		INSERT INTO posts (author_id, title, content)
		VALUES ($1, $2,$3) 
		RETURNING id;
		`,
		post.AuthorID, post.Title, post.Content,
	)
	return err
}

func (s *TaskStorage) UpdatePost(post storage.Post) error {
	_, err := s.db.Exec(context.Background(), `
	UPDATE posts 
	SET title = $1, content = $2, author_id = $3  
	WHERE id = $4;`,
		post.Title, post.Content, post.AuthorID, post.ID)
	if err != nil {
		log.Println("Ошибка обновления поста:")
		log.Println(err)
		return err
	}
	return nil
}

func (s *TaskStorage) DeletePost(post storage.Post) error {
	_, err := s.db.Exec(context.Background(), ` 
	DELETE FROM posts WHERE id = $1`,
		post.ID)
	if err != nil {
		return err
	}
	return nil
}
