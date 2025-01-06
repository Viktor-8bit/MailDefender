package services

import (
	"context"
	"goKFKa/vrscaner"
	"log"
	"time"
)

type KafkaService interface {
	Processing(ctx context.Context, message map[string]interface{})
}

type UrlkafkaService struct {
	VTS *vrscaner.VTScaner
}

func NewUrlKafkaService(token string) *UrlkafkaService {
	vts := vrscaner.NewVTScaner(token)
	return &UrlkafkaService{VTS: vts}
}

func (s *UrlkafkaService) Processing(ctx context.Context, message map[string]interface{}) {
	select {
	default:
		log.Print("получен URL: ", message["url"])

		url, ok := message["url"].(string)

		if !ok {
			log.Println("не удалось обработать url")
			return
		}

		urlID, err := s.VTS.ScanURL(ctx, url)

		if err != nil {
			log.Println(err)
			return
		}

		report, err := s.VTS.GetReport(ctx, urlID)

		if len(report.Results) == 0 {
			time.Sleep(30 * time.Second)
			report, err = s.VTS.GetReport(ctx, urlID)
		}

		log.Println("id ресурса", report.UrlID)
		log.Println("дата сканирования", report.ScanDate)
		log.Println("результаты сканирования", report.Results)

		return
	case <-ctx.Done():
		return
	}
}
