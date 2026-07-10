const mongoose = require('mongoose');
let isConnected = false;

async function connectMongo(){
    if(isConnected) return;

    try{
        await mongoose.connect(process.env.MONGO_URI || 'mongodb://localhost:27017/messagequeue')
        isConnected=true;
        console.log('[MongoDB ] Connected');
    } catch(err){
        console.error('[MongoDB] Connection failed:', err.message);
        await new Promise((r) => setTimeout(r,5000));
        return connectMongo();
    }
}

module.exports = connectMongo;