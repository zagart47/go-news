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
	s := Storage{
		db: client,
	}
	return &s, nil
}

func (s *Storage) Posts() ([]storage.Post, error) {
	var posts []storage.Post
	f := bson.D{}
	postsCol, err := s.db.Database("go-news").Collection("posts").Find(context.Background(), f)
	defer postsCol.Close(context.Background())

	if err != nil {
		return nil, err
	}
	for postsCol.Next(context.Background()) {
		var post storage.Post
		err = postsCol.Decode(&post)
		if err != nil {
			return nil, err
		}
		f := bson.M{"id": post.AuthorID}
		author := s.db.Database("go-news").Collection("authors").FindOne(context.Background(), f)
		authors := &Authors{}
		author.Decode(authors)
		post.AuthorName = authors.Name
		posts = append(posts, post)

	}
	if postsCol.Err() != nil {
		log.Println("Posts getting error:", postsCol.Err().Error())
	}
	return posts, postsCol.Err()

}

func (s *Storage) AddPost(post storage.Post) error {
	post.CreatedAt = time.Now().Unix()
	if !checkAuthorID(post.AuthorID, s.db) {
		return fmt.Errorf("post adding error, author not found: %v", post.AuthorID)
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
	if !checkAuthorID(post.AuthorID, s.db) {
		log.Println("Ошибка обновления поста, автора с таким id не существует:", post.AuthorID)
		return nil
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
	count := &Counter{}
	updateResult.Decode(count)

	return count.Seq
}

func checkAuthorID(id int, client *mongo.Client) bool {
	filterAuthorID := bson.M{"id": id}
	cur := client.Database("go-news").Collection("authors").FindOne(context.Background(), filterAuthorID)
	if cur.Err() != nil {
		return false
	}
	authors := &Authors{}
	cur.Decode(authors)
	return true
}
