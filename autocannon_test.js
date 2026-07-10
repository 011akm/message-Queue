const autocannon = require('autocannon');

const instance = autocannon({
    url: 'http://localhost:3000/jobs',
    connections: 10,
    duration: 5,
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body : JSON.stringify({
        type: 'send_email',
        payload: {to: 'test@example.com'}
    })
}, (err, result) =>{
    if(err){
        console.log('Error:', err);
        return;
    }
    console.log('Results:', result);
});

autocannon.track(instance,{renderProgressBar:true});