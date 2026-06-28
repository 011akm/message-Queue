const express = require('express');
const Redis = require('ioredis');

const app = express();
app.use(express.json());

const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
})

const STREAM_KEY = 'jobs:stream';

app.post("/jobs" , async(req,res) =>{
    const {type,payload} =req.body;
    if(!type){
        return res.status(400).json({
            error: 'job type is required'
        });
    }

    try{
        const messageId = await redis.xadd(
            STREAM_KEY,
            '*',
            'type', type,
            'payload', JSON.stringify(payload || {}),
            'createdAt',Date.now().toString()
        );

        console.log(`[Producer] Job Added -> ${messageId}, type:${type}`);
        res.status(201).json({
            status: 'queued',
            messageId,
            type,
        });

    }catch(err) {
        console.error('[Producer] failed to add job:', err);
        res.status(500).json({error: 'failed to queue job'});
    }
});

app.get('/jobs/stream-info', async(req,res) =>{
    try{
        const length = await redis.xlen(STREAM_KEY);
        res.json({
            stream : STREAM_KEY,
            length
        });
    } catch(err){
        res.json(500).json({
            error : 'failed to fetch stream info'
        });
    }
});

const PORT = process.env.PRODUCER_PORT || 3000;
app.listen(PORT, ()=>{
    console.log(`[Producer] Listening on port ${PORT}`);
});
