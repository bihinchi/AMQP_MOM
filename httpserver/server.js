var http = require('http')
var fs = require('fs')

const FILEPATH = "shared/logs.txt"

http.createServer(function (request, response) {
	
	try {
		response.writeHead(200)
		fs.createReadStream(FILEPATH).pipe(response)
	} catch (error) {
		response.writeHead(404)
		res.end();
	}
	
}).listen(8080) 