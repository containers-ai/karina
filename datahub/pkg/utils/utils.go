package utils

import (
	"strconv"
	"time"

	metric_v1alpha2 "github.com/containers-ai/api/datahub/metric/v1alpha2"
	"github.com/containers-ai/karina/datahub/pkg/repository/influxdb"
	logUtil "github.com/containers-ai/karina/pkg/utils/log"
	"github.com/golang/protobuf/ptypes/timestamp"
)

var (
	utilsScope = logUtil.RegisterScope("utils", "utils", 0)
)

type StringStringMap map[string]string

func (m StringStringMap) ReplaceKeys(old, new []string) StringStringMap {

	for i, oldKey := range old {
		if v, exist := m[oldKey]; exist {
			newKey := new[i]
			delete(m, oldKey)
			m[newKey] = v
		}
	}

	return m
}

// ParseTime parses time string to Time
func ParseTime(timeStr string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, timeStr)

	return t, err
}

// NanoSecondToSecond translate nano seconds to seconds
func NanoSecondToSecond(nanosecond int64) int64 {
	return nanosecond / 1000000000
}

// GetSampleInstance get Sample instance
func GetSampleInstance(timeObj *time.Time, numVal string) *metric_v1alpha2.Sample {
	seconds := timeObj.Unix()
	if timeObj != nil {
		return &metric_v1alpha2.Sample{
			Time: &timestamp.Timestamp{
				Seconds: seconds,
			},
			NumValue: numVal,
		}
	}
	return &metric_v1alpha2.Sample{
		NumValue: numVal,
	}
}

// GetTimeIdxFromColumns get index of time column
func GetTimeIdxFromColumns(columns []string) int {
	for idx, column := range columns {
		if column == influxdb.Time {
			return idx
		}
	}
	return 0
}

// TimeStampToNanoSecond get nano seconds from timestamp object
func TimeStampToNanoSecond(timestamp *timestamp.Timestamp) int64 {
	return timestamp.GetSeconds()*1000000000 + int64(timestamp.GetNanos())
}

// StringToInt64 parse str to int64
func StringToInt64(str string) (int64, error) {

	if val, err := strconv.ParseInt(str, 10, 64); err == nil {
		return val, err
	}

	if val, err := strconv.ParseFloat(str, 64); err == nil {
		return int64(val), err
	} else {
		return 0, err
	}
}

// StringToFloat64 parse str to int64
func StringToFloat64(str string) (float64, error) {
	return strconv.ParseFloat(str, 64)
}
