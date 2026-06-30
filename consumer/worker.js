const Redis = require('ioredis');
const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
});

const STREAM_KEY = 'jobs:stream';
let lastId=$;

function processJob(id, fields){
    const data = {};
    for(let i=0;i<fields.length;i+=2){
        data[fields[i]] = fields[i+1];
    }

    console.log(`[Consumer] Processing job ${id}:`,{
        type: data.type,
        payload: JSON.parse(data.payload || '{}'),
        createdAt: data.createdAt
    })
}

async function consumeLoop(){
    console.log('[Consumer] Starting consume loop...');

    while(true){
        try{
            const results = await redis.xread(
                'BLOCK', 5000,
                'STREAMS', STREAM_KEY, lastId
            );

            if(!results){
                continue;
            }

            const[,entries]=results[0];
            for(const [id, fields] of entries){
                processJob(id,fields);
                lastId = id;
            }
        } catch(err){
            console.error('[Consumer] Error in consume loop:', err);

            await new Promise((r) => setTimeout(r,1000));
        }
    }
}

consumeLoop();