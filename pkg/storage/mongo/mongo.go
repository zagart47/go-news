package mongo

import (
	"GoNews/pkg/storage"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type Storage struct {
	db *mongo.Client
}
type Counter struct {
	ID  string
	Seq int
}
type Authors struct {
	ID   int
	Name string
}

func New(constr string) (*Storage, error) {
	mongoOpts := options.Client().ApplyURI(constr)
	client, err := mongo.Connect(context.Background(), mongoOpts)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			client.Disconnect(context.Background())
		}
	}()
	s := &Storage{
		db: client,
	}
	return s, nil
}

func (s *Storage) Posts() ([]storage.Post, error) {
	var posts []storage.Post
	f := bson.D{}
	postsCol, err := s.db.Database("go-news").Collection("posts").Find(context.Background(), f)
	if err != nil {
		return nil, err
	}
	defer postsCol.Close(context.Background())

	for postsCol.Next(context.Background()) {
		var post storage.Post
		err = postsCol.Decode(&post)
		if err != nil {
			return nil, err
		}
		f := bson.M{"id": post.AuthorID}
		author := s.db.Database("go-news").Collection("authors").FindOne(context.Background(), f)
		if author.Err() != nil {
			return nil, author.Err()
		}
		authors := &Authors{}
		err = author.Decode(authors)
		if err != nil {
			return nil, err
		}
		post.AuthorName = authors.Name
		posts = append(posts, post)
	}
	if err := postsCol.Err(); err != nil {
		log.Println("Posts getting error:", err.Error())
		return nil, err
	}
	return posts, nil
}

func (s *Storage) AddPost(post storage.Post) error {
	post.CreatedAt = time.Now().Unix()
	if err := checkAuthorID(post.AuthorID, s.db); err != nil {
		return fmt.Errorf("post adding error, author not found: %v", err)
	}
	id := getNextSequence(s.db, "posts")
	post.ID = id
	_, err := s.db.Database("go-news").Collection("posts").InsertOne(context.Background(), post)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) UpdatePost(post storage.Post) error {
	if err := checkAuthorID(post.AuthorID, s.db); err != nil {
		log.Println("Ошибка обновления поста, автора с таким id не существует:", err)
		return err
	}

	filter := bson.M{"id": post.ID}
	_, err := s.db.Database("go-news").Collection("posts").ReplaceOne(context.Background(), filter, post)
	if err != nil {
		log.Println("Ошибка обновления поста:", err.Error())
		return err
	}
	return nil

}

func (s *Storage) DeletePost(p storage.Post) error {
	filter := bson.M{"id": p.ID}
	_, err := s.db.Database("go-news").Collection("posts").DeleteOne(context.Background(), filter)
	if err != nil {
		log.Println("Ошибка удаления поста:", err.Error())
		return err

	}
	return nil
}

func getNextSequence(client *mongo.Client, name string) int {
	postsCol := client.Database("go-news").Collection("counters")

	f := bson.M{"_id": name}
	u := bson.M{"$inc": bson.M{"seq": 1}}

	after := options.After
	opts := options.FindOneAndUpdateOptions{
		ReturnDocument: &after,
	}

	updateResult := postsCol.FindOneAndUpdate(context.Background(), f, u, &opts)
	if updateResult.Err() != nil {
		log.Println("Error getting sequence:", updateResult.Err())
		return 0
	}

	count := &Counter{}
	err := updateResult.Decode(count)
	if err != nil {
		log.Println("Error decoding sequence:", err)
		return 0
	}

	return count.Seq
}

func checkAuthorID(id int, client *mongo.Client) error {
	filterAuthorID := bson.M{"id": id}
	cur := client.Database("go-news").Collection("authors").FindOne(context.Background(), filterAuthorID)
	if cur.Err() != nil {
		return fmt.Errorf("author not found: %v", cur.Err())
	}
	authors := &Authors{}
	err := cur.Decode(authors)
	if err != nil {
		return fmt.Errorf("error decoding author: %v", err)
	}
	return nil
}
