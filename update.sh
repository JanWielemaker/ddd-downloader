#!/bin/bash

# Copyright 2011-2014, University of Amsterdam. This program is free software:
# you can redistribute it and/or modify it under the terms of the GNU Lesser 
# General Public License as published by the Free Software Foundation, either 
# version 3 of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
# for more details.
# 
# You should have received a copy of the GNU Lesser General Public License 
# along with this program. If not, see <http://www.gnu.org/licenses/>.

oai_server=http://services.kb.nl/mdo/oai
# key.sh contains your access key, e.g. key=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
source key.sh
server=${oai_server}/${key}
last=

while true; do
	if [ -z "$last" ]; then
		if [ -z "$(find . -name '*.xml.gz' -print)" ]; then
			curl --compress "$server?verb=ListRecords&set=DDD&metadataPrefix=didl" > records.xml
		else
			last=$(ls *.xml.gz -t | head -n 1)
		fi
	fi

	if [ ! -z "$last" ]; then
		startToken=$(echo $last | sed -e 's/\.xml.gz//' -e 's/_/!/g')
		echo "Starting at token: $startToken"
		curl --compress "$server?verb=ListRecords&set=DDD&metadataPrefix=didl&resumptionToken=$startToken" > records.xml
	fi

	token=`tail records.xml | sed -n '/<resumptionToken>\(.*\)<\/resumptionToken>/p' | sed 's/^.*<resumptionToken>\(.*\)<\/resumptionToken>.*$/\1/' | sed 's/!/_/g'`
	if [ "$token" == "" ]; then
		from=`echo $startToken | cut -c 5-28`
		nextToken=$startToken
		while true; do
			nextToken=`python next_token.py $nextToken`
			if [ "$nextToken" == "" ]; then
				token=""
				break
			fi
			echo "Trying alternative approach for token: $startToken till $nextToken"
			until=`echo $nextToken | cut -c 5-28`
			curl --compress "$server?verb=ListRecords&set=DDD&metadataPrefix=didl&from=$from&until=$until" > records.xml
			match=`sed -n '/noRecordsMatch/p' records.xml | wc -c`
			if [ $match == 0 ]; then 
				token=`echo $nextToken | sed 's/!/_/g'`
				break
			fi
			match=`sed -n '/<OAI-PMH/p' records.xml | wc -c`
			if [ $match == 0 ]; then 
				token=""
				break
			fi
		done
	fi
	if [ "$token" == "" ]; then
		tail records.xml | mail j.wielemaker@cwi.nl -s "Harvesting done..."
		break
	else
		gzip records.xml
		last="$token.xml.gz"
		mv records.xml.gz $last
	fi
done


# If fail files:
# Take date from last good resumptionToken, e.g.: 2012-02-14T01:08:01.951Z
# Get time range from then till a bit later.
# curl --compress "http://services.kb.nl/mdo/oai?verb=ListRecords&set=DDD&metadataPrefix=didl&from=2012-02-14T01:08:01.951Z&until=2012-02-14T01:08:02.951Z" > missed.xml
# curl --compress "http://services.kb.nl/mdo/oai?verb=ListRecords&set=DDD&metadataPrefix=didl&from=2012-02-14T01:08:02.951Z" > records.xml
# For error see: http://services.kb.nl/mdo/oai?verb=ListRecords&set=DDD&metadataPrefix=didl&resumptionToken=DDD!2012-02-14T01:08:01.951Z!!didl!0
