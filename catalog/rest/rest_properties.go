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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package rest
import (
	"crypto/tls"
	"fmt"
	"maps"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

type configResponse struct {
	Defaults  iceberg.Properties `json:"defaults"`
	Overrides iceberg.Properties `json:"overrides"`
	Endpoints []string           `json:"endpoints,omitempty"`
}

func fromProps(props iceberg.Properties, o *options) error {
	// The OAuth token endpoint may be configured via either the portable
	// oauth2-server-uri key or the legacy rest.authorization-url alias. Capture
	// both and resolve precedence after the loop, since map iteration order is
	// non-deterministic.
	var authURL, oauth2ServerURI *url.URL
	for k, v := range props {
		switch k {
		case keyOauthToken:
			if o.oauthToken == "" {
				o.oauthToken = v
			}
		case keyWarehouseLocation:
			o.warehouseLocation = v
		case keyMetadataLocation:
			o.metadataLocation = v
		case keyRestSigV4:
			o.enableSigv4 = strings.ToLower(v) == "true"
		case keyRestSigV4Region:
			o.sigv4Region = v
		case keyRestSigV4Service:
			o.sigv4Service = v
		case keyAuthUrl:
			u, err := parseAuthURL(keyAuthUrl, v)
			if err != nil {
				return err
			}
			authURL = u
		case keyOAuth2ServerURI:
			u, err := parseAuthURL(keyOAuth2ServerURI, v)
			if err != nil {
				return err
			}
			oauth2ServerURI = u
		case keyOauthCredential:
			o.credential = v
		case keyScope:
			o.scope = v
		case keyAudience:
			o.audience = v
		case keyResource:
			o.resource = v
		case keyPrefix:
			o.prefix = v
		case keyTlsSkipVerify:
			verify := strings.ToLower(v) == "true"
			if o.tlsConfig == nil {
				o.tlsConfig = &tls.Config{
					InsecureSkipVerify: verify,
				}
			} else {
				o.tlsConfig.InsecureSkipVerify = verify
			}
		case "uri", "type":
		default:
			if v != "" {
				if o.additionalProps == nil {
					o.additionalProps = iceberg.Properties{}
				}
				o.additionalProps[k] = v
			}
		}
	}

	// oauth2-server-uri is the portable, preferred key and takes precedence over
	// the rest.authorization-url alias when both are set.
	switch {
	case oauth2ServerURI != nil:
		o.authUri = oauth2ServerURI
	case authURL != nil:
		o.authUri = authURL
	}

	return nil
}

// resolveAuthURLAlias collapses the oauth2-server-uri / rest.authorization-url
// alias within a single configuration layer into the canonical
// oauth2-server-uri key. oauth2-server-uri wins over the rest.authorization-url
// alias within the same layer. Collapsing per layer before merging keeps
// defaults -> client -> overrides precedence intact.
func resolveAuthURLAlias(props iceberg.Properties) {
	v, ok := props[keyOAuth2ServerURI]
	if !ok {
		v, ok = props[keyAuthUrl]
	}
	delete(props, keyAuthUrl)
	if ok {
		props[keyOAuth2ServerURI] = v
	}
}

func parseAuthURL(key, raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("invalid %s %q: missing scheme", key, raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid %s %q: missing host", key, raw)
	}

	return u, nil
}

func toProps(o *options) iceberg.Properties {
	props := iceberg.Properties{}
	maps.Copy(props, o.additionalProps)

	setIf := func(key, v string) {
		if v != "" {
			props[key] = v
		}
	}

	setIf(keyOauthToken, o.oauthToken)
	setIf(keyOauthCredential, o.credential)
	setIf(keyWarehouseLocation, o.warehouseLocation)
	setIf(keyMetadataLocation, o.metadataLocation)
	if o.enableSigv4 {
		props[keyRestSigV4] = "true"
		setIf(keyRestSigV4Region, o.sigv4Region)
		setIf(keyRestSigV4Service, o.sigv4Service)

		// Best-effort fallback: propagate the SigV4 signing region as
		// a client region hint so that S3 I/O can determine the correct
		// regional endpoint when s3.region is not explicitly set.
		// Only applied for S3/S3Tables services where the signing region
		// is likely to match the bucket region.
		if o.sigv4Region != "" && (o.sigv4Service == "s3" || o.sigv4Service == "s3tables") {
			if _, ok := props[iceio.S3ClientRegion]; !ok {
				props[iceio.S3ClientRegion] = o.sigv4Region
			}
		}
	}

	setIf(keyPrefix, o.prefix)
	if o.authUri != nil {
		// Advertise the endpoint only under the portable oauth2-server-uri key.
		// We canonicalize to this key everywhere else (fromProps precedence,
		// resolveAuthURLAlias), so emitting the legacy rest.authorization-url
		// alias too would leave the endpoint under two names in r.props and any
		// table config cloned from it - two sources of truth that can drift.
		setIf(keyOAuth2ServerURI, o.authUri.String())
	}

	return props
}
