const http = require('http');

const data = JSON.stringify({ clientId: 'client-a' });

const options = {
  hostname: 'localhost',
  port: 8080,
  path: '/v1/check',
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer my-secret-key-123',
    'Content-Length': data.length
  }
};

const req = http.request(options, (res) => {
  let body = '';
  res.on('data', (d) => { body += d; });
  res.on('end', () => { console.log(body); });
});

req.on('error', (e) => { console.error(e); });
req.write(data);
req.end();
