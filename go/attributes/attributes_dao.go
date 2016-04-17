/*
 * bakufu
 *
 * Copyright (c) 2016 STNMRSHX
 * Licensed under the WTFPL license.
 */

package attributes

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/stnmrshx/golib/log"
	"github.com/stnmrshx/golib/sqlutils"
	"github.com/stnmrshx/bakufu/go/db"
)

func readResponse(res *http.Response, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.Status == "500" {
		return body, errors.New("Response Status 500")
	}

	return body, nil
}

func SetHostAttributes(hostname string, attributeName string, attributeValue string) error {
	_, err := db.ExecBakufu(`
			replace 
				into host_attributes (
					hostname, attribute_name, attribute_value, submit_timestamp, expire_timestamp
				) VALUES (
					?, ?, ?, NOW(), NULL
				)
			`,
		hostname,
		attributeName,
		attributeValue,
	)
	if err != nil {
		return log.Errore(err)
	}

	return err
}

func getHostAttributesByClause(whereClause string, args []interface{}) ([]HostAttributes, error) {
	res := []HostAttributes{}
	query := fmt.Sprintf(`
		select 
			hostname, 
			attribute_name, 
			attribute_value,
			submit_timestamp ,
			ifnull(expire_timestamp, '') as expire_timestamp  
		from 
			host_attributes
		%s
		order by
			hostname, attribute_name
		`, whereClause)

	err := db.QueryBakufu(query, args, func(m sqlutils.RowMap) error {
		hostAttributes := HostAttributes{}
		hostAttributes.Hostname = m.GetString("hostname")
		hostAttributes.AttributeName = m.GetString("attribute_name")
		hostAttributes.AttributeValue = m.GetString("attribute_value")
		hostAttributes.SubmitTimestamp = m.GetString("submit_timestamp")
		hostAttributes.ExpireTimestamp = m.GetString("expire_timestamp")

		res = append(res, hostAttributes)
		return nil
	})

	if err != nil {
		log.Errore(err)
	}
	return res, err
}

func GetHostAttributesByMatch(hostnameMatch string, attributeNameMatch string, attributeValueMatch string) ([]HostAttributes, error) {
	terms := []string{}
	args := sqlutils.Args()
	if hostnameMatch != "" {
		terms = append(terms, ` hostname rlike ? `)
		args = append(args, hostnameMatch)
	}
	if attributeNameMatch != "" {
		terms = append(terms, ` attribute_name rlike ? `)
		args = append(args, attributeNameMatch)
	}
	if attributeValueMatch != "" {
		terms = append(terms, ` attribute_value rlike ? `)
		args = append(args, attributeValueMatch)
	}

	if len(terms) == 0 {
		return getHostAttributesByClause("", args)
	}
	whereCondition := fmt.Sprintf(" where %s ", strings.Join(terms, " and "))

	return getHostAttributesByClause(whereCondition, args)
}

func GetHostAttributesByAttribute(attributeName string, valueMatch string) ([]HostAttributes, error) {
	if valueMatch == "" {
		valueMatch = ".?"
	}
	whereClause := ` where attribute_name = ? and attribute_value rlike ?`

	return getHostAttributesByClause(whereClause, sqlutils.Args(attributeName, valueMatch))
}
