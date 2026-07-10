const mongoose = require('mongoose');

const jobArchiveSchema = new mongoose.Schema({
    messageId:{
        type: String,
        required: true,
        unique:true
    },

    type:{
        type: String,
        required: true
    },

    payload:{
        type: mongoose.Schema.Types.Mixed
    },

    status:{ 
        type: String, 
        enum: ['completed', 'failed'], 
        required: true
    },

    retries:{ 
        type: Number, 
        default: 0 
    },

    worker:{ 
        type: String 
    },

    reason:{ 
        type: String 
    },

    processedAt: { 
        type: Date, 
        default: Date.now 
    },

    createdAt:{   
        type: String 
    },    
})

const JobArchive = mongoose.model('JobArchive', jobArchiveSchema);

module.exports = JobArchive;