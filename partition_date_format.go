// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iceberg

import "strconv"

// partitionDateParts converts Unix days to Gregorian year, month, and day.
// It uses 400-year eras so pre-epoch dates and years beyond four digits can
// be formatted without constructing a time.Time.
func partitionDateParts(days int64) (year, month, day int64) {
	z := days + 719468
	era := z / 146097
	if z < 0 {
		era = (z - 146096) / 146097
	}
	dayOfEra := z - era*146097
	yearOfEra := (dayOfEra - dayOfEra/1460 + dayOfEra/36524 - dayOfEra/146096) / 365
	y := yearOfEra + era*400
	dayOfYear := dayOfEra - (365*yearOfEra + yearOfEra/4 - yearOfEra/100)
	monthIndex := (5*dayOfYear + 2) / 153
	d := dayOfYear - (153*monthIndex+2)/5 + 1
	m := monthIndex + 3
	if monthIndex >= 10 {
		m = monthIndex - 9
	}
	if m <= 2 {
		y++
	}

	return y, m, d
}

// formatPartitionDate renders yyyy-MM-dd, optionally followed by -HH.
// Like time.Format, it pads signed years to at least four digits.
func formatPartitionDate(days int64, hour int64, withHour bool) string {
	year, month, day := partitionDateParts(days)
	var buf [32]byte
	dst := buf[:0]

	if year >= 0 && year <= 9999 {
		dst = append(dst,
			byte('0'+year/1000), byte('0'+year/100%10),
			byte('0'+year/10%10), byte('0'+year%10))
	} else {
		if year < 0 {
			dst = append(dst, '-')
			year = -year
		}
		if year < 1000 {
			dst = append(dst, '0')
			if year < 100 {
				dst = append(dst, '0')
				if year < 10 {
					dst = append(dst, '0')
				}
			}
		}
		dst = strconv.AppendInt(dst, year, 10)
	}

	dst = append(dst, '-', byte('0'+month/10), byte('0'+month%10),
		'-', byte('0'+day/10), byte('0'+day%10))
	if withHour {
		dst = append(dst, '-', byte('0'+hour/10), byte('0'+hour%10))
	}
	return string(dst)
}

func formatDayPartition(days int32) string {
	return formatPartitionDate(int64(days), 0, false)
}

func formatHourPartition(hours int32) string {
	days := int64(hours) / 24
	hour := int64(hours) % 24
	if hour < 0 {
		days--
		hour += 24
	}
	return formatPartitionDate(days, hour, true)
}
