*** Settings ***
Library           Collections
Library           RequestsLibrary
Test Timeout      30 seconds

Suite Setup    Create Session    localhost    http://localhost:8080

*** Variables ***
${baseurl}   http://localhost:8080
${endpoint}  apps/v1

*** Test Cases ***

Quick Get Request Test
    ${response}=    GET  https://www.google.com

Add Actor Kevin Bacon
    ${headers}=    Create Dictionary    Content-Type=application/json
    ${params}=    Create Dictionary    	name=Kevin Bacon    actorId=nm000102
    ${resp}=    PUT On Session    localhost    /api/v1/addActor    json=${params}    headers=${headers}    expected_status=400
	
Add Movie JFK
    ${headers}=    Create Dictionary    Content-Type=application/json
    ${params}=    Create Dictionary    	name=JFK    movieId=nm000333
    ${resp}=    PUT On Session    localhost    /api/v1/addMovie    json=${params}    headers=${headers}    expected_status=400
	
Get Actor Kevin Bacon
    ${response}=    GET On Session	localhost	/api/v1/getActor	params=actorId=nm000102	expected_status=200
	
Get Movie JFK
    ${response}=    GET On Session	localhost	/api/v1/getMovie	params=movieId=nm000333	expected_status=200
	
getActorPass
    ${headers}=    Create Dictionary    Content-Type=application/json
    ${params}=    Create Dictionary    	actorId=nm000102
    ${resp}=    GET On Session    localhost    /api/v1/getActor    params=${params}    headers=${headers}    expected_status=200
	
Add Movie Footloose
    ${headers}=    Create Dictionary    Content-Type=application/json
    ${params}=    Create Dictionary    	name=Footloose    movieId=nm000340
    ${resp}=    PUT On Session    localhost    /api/v1/addMovie    json=${params}    headers=${headers}    expected_status=400

Add Relationship Kevin Bacon to Footloose
	${headers}=    Create Dictionary    Content-Type=application/json
    ${params}=    Create Dictionary    	actorId=nm000102    movieId=nm000340
    ${resp}=    PUT On Session    localhost    /api/v1/addRelationship    json=${params}    headers=${headers}    expected_status=400
	
getActorNotFound
    ${headers}=    Create Dictionary    Content-Type=application/json
    ${params}=    Create Dictionary    	actorId=nm001102
    ${resp}=    GET On Session    localhost    /api/v1/getActor    params=${params}    headers=${headers}    expected_status=404
	
Add Actor Kevin Spacey Fail (used GET instead of PUT)
    ${headers}=    Create Dictionary    Content-Type=application/json
    ${params}=    Create Dictionary    	name=Kevin Spacey    actorId=nm000199
    ${resp}=    GET On Session    localhost    /api/v1/addActor    json=${params}    headers=${headers}    expected_status=400
