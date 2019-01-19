package score

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	score_dao "github.com/containers-ai/karina/datahub/pkg/dao/score"
	influxdb_entity_score "github.com/containers-ai/karina/datahub/pkg/entity/influxdb/score"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	influxdb_client "github.com/influxdata/influxdb/client/v2"
)

// SimulatedSchedulingScoreRepository Repository of simulated_scheduling_score data
type SimulatedSchedulingScoreRepository struct {
	influxDB *influxdb.InfluxDBRepository
}

// NewRepositoryWithConfig New SimulatedSchedulingScoreRepository with influxdb configuration
func NewRepositoryWithConfig(cfg influxdb.Config) SimulatedSchedulingScoreRepository {
	return SimulatedSchedulingScoreRepository{
		influxDB: influxdb.New(&cfg),
	}
}

// ListScoresBetweenTimes List simulated_scheduling_score data points that it's timestamp is between startTime and endTime in influxdb
func (r SimulatedSchedulingScoreRepository) ListScoresBetweenTimes(startTime, endTime *time.Time) ([]*influxdb_entity_score.SimulatedSchedulingScoreEntity, error) {

	var (
		err error

		cmd       string
		cmdSuffix string

		influxdbRows []*influxdb.InfluxDBRow

		scores = make([]*influxdb_entity_score.SimulatedSchedulingScoreEntity, 0)
	)

	lastClause, whereTimeClause := buildTimeClause(startTime, endTime)
	if whereTimeClause != "" {
		whereTimeClause = strings.TrimSuffix(whereTimeClause, "and ")
		cmdSuffix = fmt.Sprintf("where %s", whereTimeClause)
	} else {
		cmdSuffix = lastClause
	}

	cmd = fmt.Sprintf(`select * from %s %s`, string(SimulatedSchedulingScore), cmdSuffix)
	results, err := r.influxDB.QueryDB(cmd, string(influxdb.Score))
	if err != nil {
		return scores, errors.New("SimulatedSchedulingScoreRepository list scores failed: " + err.Error())
	}

	influxdbRows = influxdb.PackMap(results)
	for _, influxdbRow := range influxdbRows {
		for _, data := range influxdbRow.Data {
			scoreEntity := influxdb_entity_score.NewSimulatedSchedulingScoreEntityFromMap(data)
			scores = append(scores, &scoreEntity)
		}
	}

	return scores, nil
}

// CreateScores Create simulated_scheduling_score data points into influxdb
func (r SimulatedSchedulingScoreRepository) CreateScores(scores []*score_dao.SimulatedSchedulingScore) error {

	var (
		err error

		points = make([]*influxdb_client.Point, 0)
	)

	for _, score := range scores {

		time := score.Timestamp
		scoreBefore := score.ScoreBefore
		scoreAfter := score.ScoreAfter
		entity := influxdb_entity_score.SimulatedSchedulingScoreEntity{
			Time:        time,
			ScoreBefore: &scoreBefore,
			ScoreAfter:  &scoreAfter,
		}

		point, err := entity.InfluxDBPoint(string(SimulatedSchedulingScore))
		if err != nil {
			return errors.New("SimulatedSchedulingScoreRepository create scores failed: build influxdb point failed: " + err.Error())
		}
		points = append(points, point)
	}

	err = r.influxDB.WritePoints(points, influxdb_client.BatchPointsConfig{
		Database: string(influxdb.Score),
	})
	if err != nil {
		return errors.New("SimulatedSchedulingScoreRepository create scores failed: " + err.Error())
	}

	return nil
}

func buildTimeClause(ptrStartTime, ptrEndTime *time.Time) (string, string) {

	var (
		lastClause      string
		whereTimeClause string

		startTime = time.Now()
	)

	if ptrStartTime == nil && ptrEndTime == nil {
		lastClause = "order by time desc limit 1"
	} else {

		if ptrStartTime != nil {
			startTime = *ptrStartTime
		}

		nanoTimestampInString := strconv.FormatInt(int64(startTime.UnixNano()), 10)
		whereTimeClause = fmt.Sprintf("time > %s and ", nanoTimestampInString)

		if ptrEndTime != nil {
			nanoTimestampInString := strconv.FormatInt(int64(ptrEndTime.UnixNano()), 10)
			whereTimeClause += fmt.Sprintf("time <= %s and ", nanoTimestampInString)
		}
	}

	return lastClause, whereTimeClause
}
