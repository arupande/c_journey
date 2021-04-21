/*const mongoDetails= require('./utility').getKyrosOriginationDetails();*/
const {MongoClient, ObjectID} = require('mongodb');
const uuid = () => Math.random().toString(36).substring(2) + (new Date()).getTime().toString(36);
let client;
const checkClient = async () => {
    let url=`mongodb://localhost:27017/kyros_origination`;
    if (!client) {
        client = await MongoClient.connect(url, {
            "poolSize": 5,
            "useNewUrlParser": true,
            "useUnifiedTopology":true
        });
        return;
    }
}
exports.close = (done) => {
    if (client) {
        client.close((err, result) => {
            client = null;
            done(err);
        })
    }
};

module.exports = {
    getObjectIdByString(id) {
        return new ObjectID(id);
    },
    async getCollection(dbname, model) {
        await checkClient();
        return await client.db(dbname).collection(model);
    },
    async findOne(dbname, model, query, options) {
        await checkClient();
        return await client.db(dbname).collection(model).findOne(query, options);
    },
    async findAll(dbname, model, query,options, skip=0, limit=0, sort, isTotalNeeded) {
        await checkClient();
        let collection = await client.db(dbname).collection(model);
        let data = await collection.find(query, options).sort({"_id":-1}).toArray();
        //let total = data.length;
        return { data};
    },
    async insertOne(dbname, model, doc, options) {
//    doc._id = uuid();
        await checkClient();
        return await client.db(dbname).collection(model).insertOne(doc, options);
    },
    async updateOne(dbname, model, filter, doc, options, callback) {
        await checkClient();
        return await client.db(dbname).collection(model).updateOne(filter, doc, options);
    },
    async updateMany(dbname, model, filter, doc, options, callback) {
        await checkClient();
        return await client.db(dbname).collection(model).updateMany(filter, doc, options);
    },
    async deleteOne(dbname, model, filter, options, callback) {
        await checkClient();
        return await client.db(dbname).collection(model).deleteOne(filter, options);
    },
    async aggregate(dbname, model, query, options,isArrayRequired) {
        await checkClient();
        if(!isArrayRequired)
            return await client.db(dbname).collection(model).aggregate(query, options).toArray();
        else{
            return await client.db(dbname).collection(model).aggregate(query, options);
        }
    }
};
