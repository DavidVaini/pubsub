package pubsub

import (
	"fmt"
	"sync"

	"google.golang.org/api/option"

	ps "cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
)

var (
	client    *ps.Client
	pubsubCtx context.Context

	pubsubScopes = []string{
		ps.ScopeCloudPlatform,
		ps.ScopePubSub,
	}
)

// NewClient Generates a http.Client that is authenticated against
// Google Cloud Platform with the `scopes` provided.
func NewClient(scopes []string, email string, key string, project string) error {
	if email == "" {
		return backgrounContext(scopes, project)
	}

	return jwtContext(scopes, email, key, project)
}

func jwtContext(scopes []string, email string, key string, project string) error {
	conf := &jwt.Config{
		Email:      email,
		PrivateKey: []byte(key),
		Scopes:     scopes,
		TokenURL:   google.JWTTokenURL,
	}

	opt := option.WithHTTPClient(conf.Client(oauth2.NoContext))
	var err error
	client, err = ps.NewClient(context.Background(), project, opt)

	return err
}

func backgrounContext(scopes []string, project string) error {
	ctx := context.Background()
	c, err := google.DefaultClient(ctx, scopes...)
	if err != nil {
		return err
	}

	client, err = ps.NewClient(ctx, project, option.WithHTTPClient(c))

	return err
}

// PushMessage Will send the `msgs` to Google PubSub into the `topic` that
// is provided. If the `topic` doesn't exist, it will be created.
func PushMessage(email, key, project, topic string, msgs ...*ps.Message) error {
	var err error
	if pubsubCtx == nil {
		err = NewClient(pubsubScopes, email, key, project)
		if err != nil {
			return err
		}
	}

	var t *ps.Topic
	t, err = createTopic(topic)
	if err != nil {
		return err
	}

	var msgIDS []string
	var wg sync.WaitGroup
	errorChannel := make(chan error)
	respChannel := make(chan *ps.PublishResult)
	msgChannel := make(chan string, len(msgs))

	for w := 1; w <= 5; w++ {
		go worker(pubsubCtx, respChannel, errorChannel, msgChannel, &wg)
		wg.Add(1)
	}

	for _, msg := range msgs {
		respChannel <- t.Publish(pubsubCtx, msg)
	}

	close(respChannel)
	wg.Wait()
	close(errorChannel)
	if len(errorChannel) > 0 {
		err := <-errorChannel
		return err
	}
	close(msgChannel)

	for msgID := range msgChannel {
		msgIDS = append(msgIDS, msgID)
	}
	if len(msgIDS) != len(msgs) {
		return fmt.Errorf(
			"failed to push matching number of messages: %d:%d",
			len(msgs),
			len(msgIDS),
		)
	}

	return err
}

//Worker function to be used to process PublishResults from the new async Publish function
func worker(pubCon context.Context, resps chan *ps.PublishResult,
	res chan error, msg chan string, wg *sync.WaitGroup) {
	for {
		resp, more := <-resps
		if more {
			srvID, err := resp.Get(pubsubCtx)
			if err != nil {
				res <- err
			}
			msg <- srvID
		} else {
			break
		}
	}
	wg.Done()

}

func createTopic(topic string) (*ps.Topic, error) {
	t := client.Topic(topic)
	if t != nil {
		return t, nil
	}

	return client.CreateTopic(pubsubCtx, topic)
}
