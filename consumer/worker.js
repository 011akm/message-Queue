const Redis = require('ioredis');
const connectMongo = require('../db/mongo.js');
const JobArchive = require('../models/jobArchive.js')

const redis = new Redis({
    host: process.env.REDIS_HOST || 'localhost',
    port: process.env.REDIS_PORT || 6379,
});


const STREAM_KEY = 'jobs:stream';
const GROUP_NAME = 'workers';
const CONSUMER_NAME = process.env.CONSUMER_NAME || 'consumer-1';
const MAX_RETRIES = 3;
const IDLE_TIMEOUT_MS = 30000;

async function archiveJob(id, data, status, reason = null){

    try{
        await JobArchive.create({
            messageId: id,
            type: data.type,
            payload: data.payload,
            status,
            retries: data.retries,
            worker: CONSUMER_NAME,
            reason,
            createdAt: data.createdAt,
        });

        console.log(`[${CONSUMER_NAME}] Job ${id} archived as '${status}'`);
    } catch (err){
        if(err.code === 11000){
            return;
        }
        console.error(`[${CONSUMER_NAME}] Archive failed for ${id}:`, err.message);
    }
}

async function createGroupIfNotExists(){

    try{
        await redis.xgroup('CREATE', STREAM_KEY, GROUP_NAME, '0', 'MKSTREAM');
        console.log(`[${CONSUMER_NAME}] Consumer group '${GROUP_NAME}' created`);

    } catch(err) {
        if(err.message.includes('BUSYGROUP')){
            console.log(`[${CONSUMER_NAME}] Consumer group already exists, continuing...`)
        }else{
            throw err;
        }
    }
}

async function processJob(id, data){
    console.log(`[${CONSUMER_NAME}] Processing job ${id}:`,data);
    if(Math.random() < 0.3){
        throw new Error(`Simulated failure for job ${id}`);
    }

    await new Promise((r) => setTimeout(r,500));
    console.log(`[${CONSUMER_NAME}] Job ${id} done ✓`);
}

function parseFields(fields){
    const data={};
    for(let i=0;i<fields.length;i+=2){
        data[fields[i]]=fields[i+1];
    }
    return {
        type: data.type,
        payload: JSON.parse(data.payload || '{}'),
        createdAt: data.createdAt,
        retries: parseInt(data.retries || '0'),
    }
}

async function sendToDLQ(id,data,reason){
    await redis.xadd(
        'jobs:dlq',
        '*', 
        'originalId', id,
        'type', data.type,
        'payload',JSON.stringify(data.payload),
        'reason', reason,
        'failedAt', Date.now().toString()
    );
    console.log(`[${CONSUMER_NAME}] job ${id} moved to DLQ -- reason : ${reason}`);
}

async function recoverPendingMessage(){
    const pending = await redis.xpending(
        STREAM_KEY,
        GROUP_NAME,
        '-','+',
        10
    );

    if(pending.length == 0) return;
    console.log(`[${CONSUMER_NAME}] found ${pending.length} pending meassage(s) to recover`);
    
    for(const [id, consumer, elapsedMs] of pending){
        const claimed= await redis.xclaim(
            STREAM_KEY, 
            GROUP_NAME, 
            CONSUMER_NAME,
            5000,
            id
        )
        
        if(claimed.length > 0){
            const [, fields]= claimed[0];
            const data = parseFields(fields);

            if(data.retries >= MAX_RETRIES){
                await sendToDLQ(id,data,'max retries exceeded during recovery');
                await archiveJob(id, data, 'failed', 'max retries exceeded during recovery');
                await redis.xack(STREAM_KEY, GROUP_NAME, id);
            }else{
                console.log(`[${CONSUMER_NAME}] Recovering message ${id}, retry #${data.retries + 1}`);
                await handleMessage(id, fields);
            }
        }
    }
}

async function handleMessage(id, fields){
    const data = parseFields(fields);

    try{
        await processJob(id, data);
        await archiveJob(id, data, 'completed'); 
        await redis.xack(STREAM_KEY, GROUP_NAME, id);
    } catch(err){
        console.error(`[${CONSUMER_NAME}] job ${id} failed - ${err.message}`);

        if(data.retries + 1 >= MAX_RETRIES){
            await sendToDLQ(id, data, err.message);
            await archiveJob(id, data, 'failed', err.message);
            await redis.xack(STREAM_KEY, GROUP_NAME, id);
        }else{
            await redis.xadd(
                STREAM_KEY, '*',
                'type', data.type,
                'payload', JSON.stringify(data.payload),
                'createdAt', data.createdAt,
                'retries', (data.retries+1).toString()
            );

            await redis.xack(STREAM_KEY, GROUP_NAME, id);
            console.log(`[${CONSUMER_NAME}] job ${id} requested for retry #${data.retries + 1}`)
        }
    }
}

async function claimAbandonedJobs(){

    try{
        const pending = await redis.xpending(
          STREAM_KEY,
          GROUP_NAME,
          '-','+',
          10
        )

        for(const [id, consumer, idleMs] of pending){
            if(idleMs > IDLE_TIMEOUT_MS && consumer !== CONSUMER_NAME){
                const claimed = await redis.xclaim(
                    STREAM_KEY, 
                    GROUP_NAME, 
                    CONSUMER_NAME,
                    IDLE_TIMEOUT_MS,
                    id
                );

                if(claimed.length > 0){
                    console.log(`[${CONSUMER_NAME}] Claimed abandonde job ${id} from dead worker ${consumer}`);
                    const[, fields] = claimed[0];
                    await handleMessage(id,fields);
                }
            }
        }
    } catch(err){
        console.error(`[${CONSUMER_NAME}] Idle recovery error:`, err.message);
    }
}

async function consumeLoop(){
    await connectMongo();
    await createGroupIfNotExists();
    await recoverPendingMessage();
    
    setInterval(claimAbandonedJobs, IDLE_TIMEOUT_MS);

    console.log(`[${CONSUMER_NAME}] Listening for jobs....`);
    
    while(true){
        try{
           const results = await redis.xreadgroup(
                'GROUP', GROUP_NAME, CONSUMER_NAME,
                'COUNT', 1,
                'BLOCK', 5000,
                'STREAMS', STREAM_KEY, '>'
            );

           if(!results){
                continue;
            }

            const [, entries] = results[0];
            for(const [id, fields] of entries){
                await handleMessage(id, fields);
            }
        } catch(err){
            console.error(`[${CONSUMER_NAME}] Loop error:` , err.message);
            await new Promise((r)=> setTimeout(r,1000));
        }
    } 
}

consumeLoop();

