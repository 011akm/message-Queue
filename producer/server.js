const express = require('express');
const Redis = require('ioredis');

const app = express();
app.use(express.json());

const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
})

const STREAM_KEY = 'jobs:stream';
const GROUP_NAME = 'workers';

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
            'MAXLEN','~','1000',
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

app.get('/metrics', async (req, res) => {
  try {
    
    const queueDepth = await redis.xlen(STREAM_KEY);

    const dlqDepth = await redis.xlen('jobs:dlq');

    const pendingInfo = await redis.xpending(STREAM_KEY, GROUP_NAME, '-', '+', 100);
    const pendingCount = pendingInfo.length;

    const workerStats = {};
    for (const [id, consumer] of pendingInfo) {
      workerStats[consumer] = (workerStats[consumer] || 0) + 1;
    }

    const groupInfo = await redis.xinfo('GROUPS', STREAM_KEY);
    const group = groupInfo.find((g, i) => groupInfo[i - 1] === GROUP_NAME || g === GROUP_NAME);

    res.json({
      timestamp: new Date().toISOString(),
      queue: {
        depth: queueDepth,
        pending: pendingCount,
      },
      dlq: {
        depth: dlqDepth,
      },
      workers: workerStats,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


app.get('/dashboard', (req, res) => {
  res.sendFile(__dirname + '/dashboard.html');
});

const PORT = process.env.PRODUCER_PORT || 3000;
app.listen(PORT, ()=>{
    console.log(`[Producer] Listening on port ${PORT}`);
});
