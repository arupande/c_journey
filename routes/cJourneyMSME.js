const MongoPool = require('./mongoDB');
const fs = require('fs');
const {ObjectID} = require('mongodb');
const pmt = require('formula-pmt');
const md5 = require('md5');
const GeoPoint = require('geopoint');
let _=require('lodash');// new code
const emiEstimationConst = require("../constants/emiEstimationConst");
let {findAll} =require('../util/mongoDBConn');
let {statementExecutorAdminDB}= require('./../util/responseGenerator');//new code


var creditArray = {};
var cust_loan = {};
var crif = {};
var crif2 = {};
var s_crif = {};
var s_crif2 = {};
var prospec = [];
var borrower = [
    {
        $or : [
            {$eq: [ "$$indvReport.spouseReport", false ] },
            { "$eq": [
                    { "$ifNull": [ "$$indvReport.spouseReport", null ] },
                    null
                ]
            }
        ]
    },
    {$eq: [ "$$indvReport.status", 901 ] }
];
var spouse = [
    {$eq: [ "$$indvReport.spouseReport", true ] },
    {$eq: [ "$$indvReport.status", 901 ] }
];

function pushValue(value){
    creditArray[value["prospectId"]] = value;
}
function pushcustValue(value) {
    cust_loan[value["customerId"]] = value;
}
function pushCRIFValue(value,filterType= null) {
    updateKey(value,filterType);
    if(filterType === 'spouse'){
        s_crif[value["_id"]] = value;
    }else{
        crif[value["_id"]] = value;
    }
}
function pushCRIF2(value,filterType= null) {
    updateKey(value,filterType);
    if(filterType === 'spouse'){
        s_crif2[value["_id"]] = value;
    }else{
        crif2[value["_id"]] = value;
    }
}

function updateKey(value,filterType= null){
    if(filterType === 'spouse'){
        Object.keys(value).forEach(function(rowKey) {
            if(rowKey !== "_id"){
                value["s_"+rowKey] = value[rowKey];
                delete  value[rowKey];
            }

        });
    }
}

function pushPros(value) {
    prospec.push(value);
}
function getCreditReport(){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }

        MongoPool.getInstance(async function (client) {
            const collection = client.db("kyros_origination").collection("credit_requirements");
            // perform actions on the collection object

            let cursor = collection.aggregate([
                {"$match":{ $and: [/*{"prospectId":"5fd9781acf6b75137f04badb"},*/
                            /*{"prospectId":{$in:["60486bd0cf6b757a09266777","60477897cf6b7517192e9bf1","60476c67cf6b7517192d5ee6","6046e922cf6b751719220537","6042278bcf6b754e2d659e72","604207fecf6b754e2d6413d6","604078e5cf6b754e2d55e027","60408dcccf6b754e2d5798f5","60406be2cf6b754e2d54c223","604037edcf6b754e2d531d20","60018a81cf6b757c6ba502a3","603f64bbcf6b754e2d4f4110","603f5792cf6b754e2d4e0f41","603f425fcf6b754e2d4cb612","603de4e7cf6b753d51d39306","603df420cf6b753d51d518c9","603e3818cf6b753d51dae591","603e3424cf6b753d51dac688","603db3afcf6b753d51d02747","603b9bfacf6b7510c97f297b","60372177cf6b7522de8f36fb","603ca73ecf6b753d51c9bea3","603c9adecf6b753d51c89b9e","603b0be6cf6b7510c97d9e79","6031f05bcf6b755cd32b2adc","6039d39acf6b7510c97a2fe0","6039b60ecf6b7510c9791737","60386fc0cf6b7510c96bd986","60387ad3cf6b7510c96cd67f","60387575cf6b7510c96c8063","6038721fcf6b7510c96bf368","60379fc8cf6b7522de979cab","60378fb8cf6b7522de9702c2","60378b44cf6b7522de968b78","602c92edcf6b7508a2815d70","60377070cf6b7522de949853","60373100cf6b7522de90227c","60261b88cf6b75138c213843","603748a6cf6b7522de91f17d","60362fdccf6b7522de89e68f","60372086cf6b7522de8f2ede","60365098cf6b7522de8bbaf1","60363cfacf6b7522de8aa57b","6035efe2cf6b7522de828b27","6035f347cf6b7522de82ed7c","6035de2ecf6b7522de811abd","60352341cf6b7522de7d538e","60337c74cf6b755cd334839c","6035bfcfcf6b7522de7f0892","6035c1c6cf6b7522de7f2533","6035040bcf6b7522de7bb05f","6034fe65cf6b7522de7b5bd3","6034ef2ccf6b7522de79e148","6034d932cf6b7522de77dcce","6034df33cf6b7522de785268","6034df86cf6b7522de786862","60311869cf6b755cd3296901","6034aac0cf6b7522de73a114","60347b3fcf6b7522de6e0912","6034961fcf6b7522de71292a","602f95ffcf6b7508a2abb189","60347a59cf6b7522de6e007a","603472ffcf6b7522de6d7b31","6033c035cf6b755cd33a795e","6033b232cf6b755cd339cead","602b813acf6b7508a2753e36","6033aac6cf6b755cd339619c","60339912cf6b755cd337b897","60309059cf6b755cd32083ef","603388d7cf6b755cd335c0da","60338a5ccf6b755cd335d09b","602e425fcf6b7508a29ac580","6033534ecf6b755cd330beda","60332e80cf6b755cd32e82b8","602f7b7fcf6b7508a2a92069","602f5eb3cf6b7508a2a7298c","602b7af8cf6b7508a27468a9","602d0c14cf6b7508a28b80ea","602d0c60cf6b7508a28b85ef","6030e43ccf6b755cd326a9f8","6030872dcf6b755cd31fe1a2","60309979cf6b755cd3211f0f","60309474cf6b755cd320ced3","602d00e8cf6b7508a28a7bdc","6030a717cf6b755cd3220b46","6030ad98cf6b755cd3228d3b","60307f82cf6b755cd31f382b","6030816ecf6b755cd31f6cf6","602fc9f8cf6b7508a2ae0b9d","602faf1ccf6b7508a2acd3b0","602fad16cf6b7508a2acb9e8","602fb3dacf6b7508a2ad0049","602f5d7ccf6b7508a2a70cb8","602f867dcf6b7508a2a9c88c","602b7f94cf6b7508a274f5cf","6023d87fcf6b756955bb534c","602e1479cf6b7508a297a1ab","602f6673cf6b7508a2a7f959","602f2b56cf6b7508a2a2f3f0","602de495cf6b7508a2918a4f"]}},*/
                            {"state":901}]
                    }
                },
                {
                    "$project":{
                        prospectId:"$prospectId",
                        DemandAmount:{$toInt:"$demandAmount"},
                        ExperienceInBusinessActivites:"$experience",
                        SchemeName:"$programId",
                        statusCd:"$statusCd"
                    }
                }
            ]);

            try {
                await cursor.forEach(async function (value) {
                    pushValue(value);
                });
                resolve();
            }catch (e) {
                console.error(e);
                reject();
            }

        });

    });
}

function getCustLoan(){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }

        MongoPool.getInstance(async function (client) {
            const collection = client.db("kyros_origination").collection("customer_loan_applications");
            // perform actions on the collection object

            let cursor = collection.aggregate([
                {"$match":{ $and: [/*{"customerId":"5fd9781acf6b75137f04badb"},*/
                            /*{"customerId":{$in:["60486bd0cf6b757a09266777","60477897cf6b7517192e9bf1","60476c67cf6b7517192d5ee6","6046e922cf6b751719220537","6042278bcf6b754e2d659e72","604207fecf6b754e2d6413d6","604078e5cf6b754e2d55e027","60408dcccf6b754e2d5798f5","60406be2cf6b754e2d54c223","604037edcf6b754e2d531d20","60018a81cf6b757c6ba502a3","603f64bbcf6b754e2d4f4110","603f5792cf6b754e2d4e0f41","603f425fcf6b754e2d4cb612","603de4e7cf6b753d51d39306","603df420cf6b753d51d518c9","603e3818cf6b753d51dae591","603e3424cf6b753d51dac688","603db3afcf6b753d51d02747","603b9bfacf6b7510c97f297b","60372177cf6b7522de8f36fb","603ca73ecf6b753d51c9bea3","603c9adecf6b753d51c89b9e","603b0be6cf6b7510c97d9e79","6031f05bcf6b755cd32b2adc","6039d39acf6b7510c97a2fe0","6039b60ecf6b7510c9791737","60386fc0cf6b7510c96bd986","60387ad3cf6b7510c96cd67f","60387575cf6b7510c96c8063","6038721fcf6b7510c96bf368","60379fc8cf6b7522de979cab","60378fb8cf6b7522de9702c2","60378b44cf6b7522de968b78","602c92edcf6b7508a2815d70","60377070cf6b7522de949853","60373100cf6b7522de90227c","60261b88cf6b75138c213843","603748a6cf6b7522de91f17d","60362fdccf6b7522de89e68f","60372086cf6b7522de8f2ede","60365098cf6b7522de8bbaf1","60363cfacf6b7522de8aa57b","6035efe2cf6b7522de828b27","6035f347cf6b7522de82ed7c","6035de2ecf6b7522de811abd","60352341cf6b7522de7d538e","60337c74cf6b755cd334839c","6035bfcfcf6b7522de7f0892","6035c1c6cf6b7522de7f2533","6035040bcf6b7522de7bb05f","6034fe65cf6b7522de7b5bd3","6034ef2ccf6b7522de79e148","6034d932cf6b7522de77dcce","6034df33cf6b7522de785268","6034df86cf6b7522de786862","60311869cf6b755cd3296901","6034aac0cf6b7522de73a114","60347b3fcf6b7522de6e0912","6034961fcf6b7522de71292a","602f95ffcf6b7508a2abb189","60347a59cf6b7522de6e007a","603472ffcf6b7522de6d7b31","6033c035cf6b755cd33a795e","6033b232cf6b755cd339cead","602b813acf6b7508a2753e36","6033aac6cf6b755cd339619c","60339912cf6b755cd337b897","60309059cf6b755cd32083ef","603388d7cf6b755cd335c0da","60338a5ccf6b755cd335d09b","602e425fcf6b7508a29ac580","6033534ecf6b755cd330beda","60332e80cf6b755cd32e82b8","602f7b7fcf6b7508a2a92069","602f5eb3cf6b7508a2a7298c","602b7af8cf6b7508a27468a9","602d0c14cf6b7508a28b80ea","602d0c60cf6b7508a28b85ef","6030e43ccf6b755cd326a9f8","6030872dcf6b755cd31fe1a2","60309979cf6b755cd3211f0f","60309474cf6b755cd320ced3","602d00e8cf6b7508a28a7bdc","6030a717cf6b755cd3220b46","6030ad98cf6b755cd3228d3b","60307f82cf6b755cd31f382b","6030816ecf6b755cd31f6cf6","602fc9f8cf6b7508a2ae0b9d","602faf1ccf6b7508a2acd3b0","602fad16cf6b7508a2acb9e8","602fb3dacf6b7508a2ad0049","602f5d7ccf6b7508a2a70cb8","602f867dcf6b7508a2a9c88c","602b7f94cf6b7508a274f5cf","6023d87fcf6b756955bb534c","602e1479cf6b7508a297a1ab","602f6673cf6b7508a2a7f959","602f2b56cf6b7508a2a2f3f0","602de495cf6b7508a2918a4f"]}},*/
                            {"state":901}]}
                },
                {
                    "$project":{
                        loanApplications:"$_id",
                        customerId:"$customerId",
                        statusCd:"$loanStatus",
                        FinanceId:"$financeId",
                        TotalAmount:{$toInt:"$loanAmount"},
                        CRIFEMI:{$toInt:"$currentEMI"},
                        AgreementNo:"$loanApplicationNo",
                        ExperienceInBusinessActivites:"$experience",
                        DisbursmentDate:"$disbursementDate",
                        assignedUserId:"$assignedUserId",
                        size1:{$size:{
                                $filter: {
                                    input: "$stages",
                                    as: "stages",
                                    cond: { $and:[
                                            {$eq: [ "$$stages.statusCd", "PENDING_DEVIATION_REVIEW" ] },
                                            {$eq: [ "$$stages.stageName", "DEVIATION_REVIEW_ASSIGNMENT" ]}
                                        ]}
                                }
                            }
                        },
                        size2:{$size:{
                                $filter: {
                                    input: "$stages",
                                    as: "stages",
                                    cond: { $and:[
                                            {$eq: [ "$$stages.statusCd", "PENDING_LOAN_APPLICATION" ] },
                                            {$eq: [ "$$stages.stageName", "DEVIATION_APPROVAL" ]}
                                        ]}
                                }
                            }
                        },
                        kyc:{$size:
                                {
                                    $filter: {
                                        input: "$stages",
                                        as: "stages",
                                        cond: { $and:[
                                                {$eq: [ "$$stages.stageName", "PENDING_KYC" ]}
                                            ]}
                                    }
                                }
                        },
                        stages: { $arrayElemAt: [ { $slice: [ "$stages.stageName", -1 ] }, 0 ] }
                    }
                }
            ]);
            try {
                await cursor.forEach(async function (value) {
                    //console.log("cla:"+JSON.stringify(value))
                    pushcustValue(value);
                });
                resolve();
            }catch (e) {
                console.error(e);
                reject();
            }

        });

    });
}

function getProspects(){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }

        MongoPool.getInstance(async function (client) {
            const collection = client.db("kyros_origination").collection("prospect");
            // perform actions on the collection object

            let cursor = collection.aggregate([
                {"$match":{ $and: [/*{"_id": new ObjectID("5fd9781acf6b75137f04badb")},*/
                            /*{"_id": {$in:[new ObjectID("60486bd0cf6b757a09266777"),new ObjectID("60477897cf6b7517192e9bf1"),new ObjectID("60476c67cf6b7517192d5ee6"),new ObjectID("6046e922cf6b751719220537"),new ObjectID("6042278bcf6b754e2d659e72"),new ObjectID("604207fecf6b754e2d6413d6"),new ObjectID("604078e5cf6b754e2d55e027"),new ObjectID("60408dcccf6b754e2d5798f5"),new ObjectID("60406be2cf6b754e2d54c223"),new ObjectID("604037edcf6b754e2d531d20"),new ObjectID("60018a81cf6b757c6ba502a3"),new ObjectID("603f64bbcf6b754e2d4f4110"),new ObjectID("603f5792cf6b754e2d4e0f41"),new ObjectID("603f425fcf6b754e2d4cb612"),new ObjectID("603de4e7cf6b753d51d39306"),new ObjectID("603df420cf6b753d51d518c9"),new ObjectID("603e3818cf6b753d51dae591"),new ObjectID("603e3424cf6b753d51dac688"),new ObjectID("603db3afcf6b753d51d02747"),new ObjectID("603b9bfacf6b7510c97f297b"),new ObjectID("60372177cf6b7522de8f36fb"),new ObjectID("603ca73ecf6b753d51c9bea3"),new ObjectID("603c9adecf6b753d51c89b9e"),new ObjectID("603b0be6cf6b7510c97d9e79"),new ObjectID("6031f05bcf6b755cd32b2adc"),new ObjectID("6039d39acf6b7510c97a2fe0"),new ObjectID("6039b60ecf6b7510c9791737"),new ObjectID("60386fc0cf6b7510c96bd986"),new ObjectID("60387ad3cf6b7510c96cd67f"),new ObjectID("60387575cf6b7510c96c8063"),new ObjectID("6038721fcf6b7510c96bf368"),new ObjectID("60379fc8cf6b7522de979cab"),new ObjectID("60378fb8cf6b7522de9702c2"),new ObjectID("60378b44cf6b7522de968b78"),new ObjectID("602c92edcf6b7508a2815d70"),new ObjectID("60377070cf6b7522de949853"),new ObjectID("60373100cf6b7522de90227c"),new ObjectID("60261b88cf6b75138c213843"),new ObjectID("603748a6cf6b7522de91f17d"),new ObjectID("60362fdccf6b7522de89e68f"),new ObjectID("60372086cf6b7522de8f2ede"),new ObjectID("60365098cf6b7522de8bbaf1"),new ObjectID("60363cfacf6b7522de8aa57b"),new ObjectID("6035efe2cf6b7522de828b27"),new ObjectID("6035f347cf6b7522de82ed7c"),new ObjectID("6035de2ecf6b7522de811abd"),new ObjectID("60352341cf6b7522de7d538e"),new ObjectID("60337c74cf6b755cd334839c"),new ObjectID("6035bfcfcf6b7522de7f0892"),new ObjectID("6035c1c6cf6b7522de7f2533"),new ObjectID("6035040bcf6b7522de7bb05f"),new ObjectID("6034fe65cf6b7522de7b5bd3"),new ObjectID("6034ef2ccf6b7522de79e148"),new ObjectID("6034d932cf6b7522de77dcce"),new ObjectID("6034df33cf6b7522de785268"),new ObjectID("6034df86cf6b7522de786862"),new ObjectID("60311869cf6b755cd3296901"),new ObjectID("6034aac0cf6b7522de73a114"),new ObjectID("60347b3fcf6b7522de6e0912"),new ObjectID("6034961fcf6b7522de71292a"),new ObjectID("602f95ffcf6b7508a2abb189"),new ObjectID("60347a59cf6b7522de6e007a"),new ObjectID("603472ffcf6b7522de6d7b31"),new ObjectID("6033c035cf6b755cd33a795e"),new ObjectID("6033b232cf6b755cd339cead"),new ObjectID("602b813acf6b7508a2753e36"),new ObjectID("6033aac6cf6b755cd339619c"),new ObjectID("60339912cf6b755cd337b897"),new ObjectID("60309059cf6b755cd32083ef"),new ObjectID("603388d7cf6b755cd335c0da"),new ObjectID("60338a5ccf6b755cd335d09b"),new ObjectID("602e425fcf6b7508a29ac580"),new ObjectID("6033534ecf6b755cd330beda"),new ObjectID("60332e80cf6b755cd32e82b8"),new ObjectID("602f7b7fcf6b7508a2a92069"),new ObjectID("602f5eb3cf6b7508a2a7298c"),new ObjectID("602b7af8cf6b7508a27468a9"),new ObjectID("602d0c14cf6b7508a28b80ea"),new ObjectID("602d0c60cf6b7508a28b85ef"),new ObjectID("6030e43ccf6b755cd326a9f8"),new ObjectID("6030872dcf6b755cd31fe1a2"),new ObjectID("60309979cf6b755cd3211f0f"),new ObjectID("60309474cf6b755cd320ced3"),new ObjectID("602d00e8cf6b7508a28a7bdc"),new ObjectID("6030a717cf6b755cd3220b46"),new ObjectID("6030ad98cf6b755cd3228d3b"),new ObjectID("60307f82cf6b755cd31f382b"),new ObjectID("6030816ecf6b755cd31f6cf6"),new ObjectID("602fc9f8cf6b7508a2ae0b9d"),new ObjectID("602faf1ccf6b7508a2acd3b0"),new ObjectID("602fad16cf6b7508a2acb9e8"),new ObjectID("602fb3dacf6b7508a2ad0049"),new ObjectID("602f5d7ccf6b7508a2a70cb8"),new ObjectID("602f867dcf6b7508a2a9c88c"),new ObjectID("602b7f94cf6b7508a274f5cf"),new ObjectID("6023d87fcf6b756955bb534c"),new ObjectID("602e1479cf6b7508a297a1ab"),new ObjectID("602f6673cf6b7508a2a7f959"),new ObjectID("602f2b56cf6b7508a2a2f3f0"),new ObjectID("602de495cf6b7508a2918a4f")]}},*/
                            {"state":901},
                            { legalForm: "LEGAL_FORM_ENTITY"}
                        ]}
                },

                {
                    "$project":{
                        FirstName:"$individualAddendum.firstName",
                        LastName:"$individualAddendum.lastName",
                        MiddleName:"$individualAddendum.middleName",
                        DateOfBirth:"$individualAddendum.dateOfBirth",
                        ContactNumber:"$individualAddendum.mobileNo",
                        EducationQualification:"$individualAddendum.educationQualification",
                        CreatedDate:"$creationDate",
                        Salutation:"$individualAddendum.namePrefix",
                        Gender:"$individualAddendum.genderCd",
                        MaritalStatus:"$individualAddendum.maritalStatus",
                        BankAccountType:{$arrayElemAt: [ "$bankAccounts.accountType", -1 ]},
                        AnnualIncome :{$toInt : { $multiply: [ "$financialInformation.incomeDetails.monthlyIncome", 12 ] } },
                        FamilyDependentCount :"$individualAddendum.numDependants",
                        identifierType : "$identifiers.identifierType",
                        identifierNumber : "$identifiers.identifierNumber",
                        AddressLine1:{$arrayElemAt: [ "$addresses.addressLine1", -1 ]},
                        AddressLine2:{$arrayElemAt: [ "$addresses.addressLine2", -1 ]},
                        Area:{$arrayElemAt: [ "$addresses.village", -1 ]},
                        Town:{$arrayElemAt: [ "$addresses.subDistrict", -1 ]},
                        District:{$arrayElemAt: [ "$addresses.district", -1 ]},
                        Postcode:{$arrayElemAt: [ "$addresses.postalCode", -1 ]},
                        StateName:{$arrayElemAt: [ "$addresses.stateCd", -1 ]},
                        locationLatitude:"$locationLatitude",
                        locationLongitude:"$locationLongitude",
                        TotalMonthlyExpensesOfBorrower:{$toInt:{$arrayElemAt: [ "$financialInformation.statements.monthlyExpenses", -1 ]}},
                        TotalExistingLoansOutStandingAmount:{$toInt: {
                                $reduce: {
                                    input: {
                                        $filter: {
                                            input: "$financialInformation.liabilities",
                                            as: "liabilities",
                                            cond: { $and:[
                                                    {$eq: [ "$$liabilities.state", 901 ] },
                                                    {$eq: [ "$$liabilities.loanStatus", "LOAN_LIABILITY_STATUS_OPEN" ] }
                                                ]
                                            }
                                        }
                                    },
                                    initialValue: 0,
                                    in: {$sum: ["$$value", "$$this.outstandingAmount"]}
                                }
                            }
                        },
                        TotalCurrentEMIAmount:{$toInt: {
                                $reduce: {
                                    input: {
                                        $filter: {
                                            input: "$financialInformation.liabilities",
                                            as: "liabilities",
                                            cond: { $and:[
                                                    {$eq: [ "$$liabilities.state", 901 ] },
                                                    {$eq: [ "$$liabilities.loanStatus", "LOAN_LIABILITY_STATUS_OPEN" ] }
                                                ]
                                            }
                                        }
                                    },
                                    initialValue: 0,
                                    in: {$sum: ["$$value", "$$this.repaymentAmount"]}
                                }
                            }
                        },
                        ExistingCreditFacility: {$toInt: {
                                $reduce: {
                                    input: {
                                        $filter: {
                                            input: "$financialInformation.liabilities",
                                            as: "liabilities",
                                            cond: { $and:[
                                                    {$eq: [ "$$liabilities.state", 901 ] },
                                                    {$eq: [ "$$liabilities.loanStatus", "LOAN_LIABILITY_STATUS_OPEN" ] }
                                                ]
                                            }
                                        }
                                    },
                                    initialValue: 0,
                                    in: {$sum: ["$$value", 1]}
                                }
                            }
                        },
                        catalogFieldKey:"$catalogs.catalogData.fieldData.fieldKey",
                        catalogFieldValue:"$catalogs.catalogData.fieldData.value",
                        catalogKey:"$catalogs.catalogKey",
                    }
                }
            ]);

            try {
                await cursor.forEach(async function (value) {
                    pushPros(value);
                });
                resolve();
            }catch (e) {
                console.error(e);
                reject();
            }

        });

    });
}

function getCRIF2(filterType=null){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }

        let filter = filterType === "spouse" ? spouse : borrower;
        MongoPool.getInstance(async function (client) {
            const collection = client.db("kyros_origination").collection("crif_credit_reports");
            // perform actions on the collection object

            let cursor = collection.aggregate([
                    /*{"$match":{"_id": new ObjectID("5fd9781acf6b75137f04badb")}
                    },*/
                    /*{"$match":{"_id": {$in:[new ObjectID("60486bd0cf6b757a09266777"),new ObjectID("60477897cf6b7517192e9bf1"),new ObjectID("60476c67cf6b7517192d5ee6"),new ObjectID("6046e922cf6b751719220537"),new ObjectID("6042278bcf6b754e2d659e72"),new ObjectID("604207fecf6b754e2d6413d6"),new ObjectID("604078e5cf6b754e2d55e027"),new ObjectID("60408dcccf6b754e2d5798f5"),new ObjectID("60406be2cf6b754e2d54c223"),new ObjectID("604037edcf6b754e2d531d20"),new ObjectID("60018a81cf6b757c6ba502a3"),new ObjectID("603f64bbcf6b754e2d4f4110"),new ObjectID("603f5792cf6b754e2d4e0f41"),new ObjectID("603f425fcf6b754e2d4cb612"),new ObjectID("603de4e7cf6b753d51d39306"),new ObjectID("603df420cf6b753d51d518c9"),new ObjectID("603e3818cf6b753d51dae591"),new ObjectID("603e3424cf6b753d51dac688"),new ObjectID("603db3afcf6b753d51d02747"),new ObjectID("603b9bfacf6b7510c97f297b"),new ObjectID("60372177cf6b7522de8f36fb"),new ObjectID("603ca73ecf6b753d51c9bea3"),new ObjectID("603c9adecf6b753d51c89b9e"),new ObjectID("603b0be6cf6b7510c97d9e79"),new ObjectID("6031f05bcf6b755cd32b2adc"),new ObjectID("6039d39acf6b7510c97a2fe0"),new ObjectID("6039b60ecf6b7510c9791737"),new ObjectID("60386fc0cf6b7510c96bd986"),new ObjectID("60387ad3cf6b7510c96cd67f"),new ObjectID("60387575cf6b7510c96c8063"),new ObjectID("6038721fcf6b7510c96bf368"),new ObjectID("60379fc8cf6b7522de979cab"),new ObjectID("60378fb8cf6b7522de9702c2"),new ObjectID("60378b44cf6b7522de968b78"),new ObjectID("602c92edcf6b7508a2815d70"),new ObjectID("60377070cf6b7522de949853"),new ObjectID("60373100cf6b7522de90227c"),new ObjectID("60261b88cf6b75138c213843"),new ObjectID("603748a6cf6b7522de91f17d"),new ObjectID("60362fdccf6b7522de89e68f"),new ObjectID("60372086cf6b7522de8f2ede"),new ObjectID("60365098cf6b7522de8bbaf1"),new ObjectID("60363cfacf6b7522de8aa57b"),new ObjectID("6035efe2cf6b7522de828b27"),new ObjectID("6035f347cf6b7522de82ed7c"),new ObjectID("6035de2ecf6b7522de811abd"),new ObjectID("60352341cf6b7522de7d538e"),new ObjectID("60337c74cf6b755cd334839c"),new ObjectID("6035bfcfcf6b7522de7f0892"),new ObjectID("6035c1c6cf6b7522de7f2533"),new ObjectID("6035040bcf6b7522de7bb05f"),new ObjectID("6034fe65cf6b7522de7b5bd3"),new ObjectID("6034ef2ccf6b7522de79e148"),new ObjectID("6034d932cf6b7522de77dcce"),new ObjectID("6034df33cf6b7522de785268"),new ObjectID("6034df86cf6b7522de786862"),new ObjectID("60311869cf6b755cd3296901"),new ObjectID("6034aac0cf6b7522de73a114"),new ObjectID("60347b3fcf6b7522de6e0912"),new ObjectID("6034961fcf6b7522de71292a"),new ObjectID("602f95ffcf6b7508a2abb189"),new ObjectID("60347a59cf6b7522de6e007a"),new ObjectID("603472ffcf6b7522de6d7b31"),new ObjectID("6033c035cf6b755cd33a795e"),new ObjectID("6033b232cf6b755cd339cead"),new ObjectID("602b813acf6b7508a2753e36"),new ObjectID("6033aac6cf6b755cd339619c"),new ObjectID("60339912cf6b755cd337b897"),new ObjectID("60309059cf6b755cd32083ef"),new ObjectID("603388d7cf6b755cd335c0da"),new ObjectID("60338a5ccf6b755cd335d09b"),new ObjectID("602e425fcf6b7508a29ac580"),new ObjectID("6033534ecf6b755cd330beda"),new ObjectID("60332e80cf6b755cd32e82b8"),new ObjectID("602f7b7fcf6b7508a2a92069"),new ObjectID("602f5eb3cf6b7508a2a7298c"),new ObjectID("602b7af8cf6b7508a27468a9"),new ObjectID("602d0c14cf6b7508a28b80ea"),new ObjectID("602d0c60cf6b7508a28b85ef"),new ObjectID("6030e43ccf6b755cd326a9f8"),new ObjectID("6030872dcf6b755cd31fe1a2"),new ObjectID("60309979cf6b755cd3211f0f"),new ObjectID("60309474cf6b755cd320ced3"),new ObjectID("602d00e8cf6b7508a28a7bdc"),new ObjectID("6030a717cf6b755cd3220b46"),new ObjectID("6030ad98cf6b755cd3228d3b"),new ObjectID("60307f82cf6b755cd31f382b"),new ObjectID("6030816ecf6b755cd31f6cf6"),new ObjectID("602fc9f8cf6b7508a2ae0b9d"),new ObjectID("602faf1ccf6b7508a2acd3b0"),new ObjectID("602fad16cf6b7508a2acb9e8"),new ObjectID("602fb3dacf6b7508a2ad0049"),new ObjectID("602f5d7ccf6b7508a2a70cb8"),new ObjectID("602f867dcf6b7508a2a9c88c"),new ObjectID("602b7f94cf6b7508a274f5cf"),new ObjectID("6023d87fcf6b756955bb534c"),new ObjectID("602e1479cf6b7508a297a1ab"),new ObjectID("602f6673cf6b7508a2a7f959"),new ObjectID("602f2b56cf6b7508a2a2f3f0"),new ObjectID("602de495cf6b7508a2918a4f")]}}},*/
                    {
                        "$project":{
                            "indvReportFiles":
                                {
                                    $filter: {
                                        input: "$indvReportFiles",
                                        as: "indvReport",
                                        cond: { $and : filter
                                        }
                                    }
                                }
                        }
                    },
                    {"$project":{"indvReportFiles":{$arrayElemAt: [ "$indvReportFiles", -1 ]}}},
                    {"$unwind":{"path":"$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response","preserveNullAndEmptyArrays": true}},
                    {
                        "$group":{
                            "_id":{_id:"$_id","indv":"$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse","grp":"$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse"},
                            "resInstalment": {
                                "$push": {
                                    "$cond":[
                                        { $not: ["$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.noEMI"] },
                                        //{$ne : ["$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.maxDPD", undefined]},
                                        null,
                                        {"$ifNull":["$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.installmentAmt","NA"]}

                                    ]
                                }
                            },
                        }
                    }
                    ,
                    {
                        "$project":{
                            "_id":"$_id._id",
                            "indv":"$_id.indv",
                            "grp":"$_id.grp",
                            "resInstalment":"$resInstalment"
                        }
                    },
                    {"$unwind":{"path":"$indv","preserveNullAndEmptyArrays": true}},
                    {
                        "$group":{
                            "_id":{_id:"$_id","resInstalment":"$resInstalment","grp":"$grp"},
                            "indvInstalment": {
                                "$push": {
                                    "$cond":[
                                        { $not: ["$grp.loanDetail.noEMI"] },
                                        //{$ne : ["$indv.loanDetail.noEMI", undefined]},
                                        null,
                                        {"$ifNull":["$indv.loanDetail.installmentAmt","NA"]}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "$project":{
                            "_id":"$_id._id",
                            "indvInstalment":"$indvInstalment",
                            "grp":"$_id.grp",
                            "resInstalment":"$_id.resInstalment"
                        }
                    },
                    {"$unwind":{"path":"$grp","preserveNullAndEmptyArrays": true}},
                    {
                        "$group":{
                            "_id":{_id:"$_id","resInstalment":"$resInstalment","indvInstalment":"$indvInstalment"},
                            "grpInstalment": {
                                "$push": {
                                    "$cond":[
                                        { $not: ["$grp.loanDetail.noEMI"] },
                                        //{$ne : ["$grp.loanDetail.maxDPD", undefined]},
                                        null,
                                        {"$ifNull":["$grp.loanDetail.installmentAmt","NA"]}
                                    ]
                                }
                            }
                        }
                    },
                    {
                        "$project":{
                            "_id":"$_id._id",
                            "grpInstalment":"$grpInstalment",
                            "indvInstalment":"$_id.indvInstalment",
                            "resInstalment":"$_id.resInstalment"
                        }
                    }
                ],
                {
                    allowDiskUse: true
                }
            );

            try {
                await cursor.forEach(async function (value) {
                    //console.log(value);
                    pushCRIF2(value,filterType);
                });
                resolve();
            }catch (e) {
                console.error(e);
                reject();
            }

        });

    });
}

function getCRIFData(filterType = null){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }
        let filter = filterType === "spouse" ? spouse : borrower;
        MongoPool.getInstance(async function (client) {
            const collection = client.db("kyros_origination").collection("crif_credit_reports");
            // perform actions on the collection object
            let cursor = collection.aggregate([
                /*{"$match":{"_id": new ObjectID("5fd9781acf6b75137f04badb")}},*/
                /*{"$match":{"_id": {$in:[new ObjectID("60486bd0cf6b757a09266777"),new ObjectID("60477897cf6b7517192e9bf1"),new ObjectID("60476c67cf6b7517192d5ee6"),new ObjectID("6046e922cf6b751719220537"),new ObjectID("6042278bcf6b754e2d659e72"),new ObjectID("604207fecf6b754e2d6413d6"),new ObjectID("604078e5cf6b754e2d55e027"),new ObjectID("60408dcccf6b754e2d5798f5"),new ObjectID("60406be2cf6b754e2d54c223"),new ObjectID("604037edcf6b754e2d531d20"),new ObjectID("60018a81cf6b757c6ba502a3"),new ObjectID("603f64bbcf6b754e2d4f4110"),new ObjectID("603f5792cf6b754e2d4e0f41"),new ObjectID("603f425fcf6b754e2d4cb612"),new ObjectID("603de4e7cf6b753d51d39306"),new ObjectID("603df420cf6b753d51d518c9"),new ObjectID("603e3818cf6b753d51dae591"),new ObjectID("603e3424cf6b753d51dac688"),new ObjectID("603db3afcf6b753d51d02747"),new ObjectID("603b9bfacf6b7510c97f297b"),new ObjectID("60372177cf6b7522de8f36fb"),new ObjectID("603ca73ecf6b753d51c9bea3"),new ObjectID("603c9adecf6b753d51c89b9e"),new ObjectID("603b0be6cf6b7510c97d9e79"),new ObjectID("6031f05bcf6b755cd32b2adc"),new ObjectID("6039d39acf6b7510c97a2fe0"),new ObjectID("6039b60ecf6b7510c9791737"),new ObjectID("60386fc0cf6b7510c96bd986"),new ObjectID("60387ad3cf6b7510c96cd67f"),new ObjectID("60387575cf6b7510c96c8063"),new ObjectID("6038721fcf6b7510c96bf368"),new ObjectID("60379fc8cf6b7522de979cab"),new ObjectID("60378fb8cf6b7522de9702c2"),new ObjectID("60378b44cf6b7522de968b78"),new ObjectID("602c92edcf6b7508a2815d70"),new ObjectID("60377070cf6b7522de949853"),new ObjectID("60373100cf6b7522de90227c"),new ObjectID("60261b88cf6b75138c213843"),new ObjectID("603748a6cf6b7522de91f17d"),new ObjectID("60362fdccf6b7522de89e68f"),new ObjectID("60372086cf6b7522de8f2ede"),new ObjectID("60365098cf6b7522de8bbaf1"),new ObjectID("60363cfacf6b7522de8aa57b"),new ObjectID("6035efe2cf6b7522de828b27"),new ObjectID("6035f347cf6b7522de82ed7c"),new ObjectID("6035de2ecf6b7522de811abd"),new ObjectID("60352341cf6b7522de7d538e"),new ObjectID("60337c74cf6b755cd334839c"),new ObjectID("6035bfcfcf6b7522de7f0892"),new ObjectID("6035c1c6cf6b7522de7f2533"),new ObjectID("6035040bcf6b7522de7bb05f"),new ObjectID("6034fe65cf6b7522de7b5bd3"),new ObjectID("6034ef2ccf6b7522de79e148"),new ObjectID("6034d932cf6b7522de77dcce"),new ObjectID("6034df33cf6b7522de785268"),new ObjectID("6034df86cf6b7522de786862"),new ObjectID("60311869cf6b755cd3296901"),new ObjectID("6034aac0cf6b7522de73a114"),new ObjectID("60347b3fcf6b7522de6e0912"),new ObjectID("6034961fcf6b7522de71292a"),new ObjectID("602f95ffcf6b7508a2abb189"),new ObjectID("60347a59cf6b7522de6e007a"),new ObjectID("603472ffcf6b7522de6d7b31"),new ObjectID("6033c035cf6b755cd33a795e"),new ObjectID("6033b232cf6b755cd339cead"),new ObjectID("602b813acf6b7508a2753e36"),new ObjectID("6033aac6cf6b755cd339619c"),new ObjectID("60339912cf6b755cd337b897"),new ObjectID("60309059cf6b755cd32083ef"),new ObjectID("603388d7cf6b755cd335c0da"),new ObjectID("60338a5ccf6b755cd335d09b"),new ObjectID("602e425fcf6b7508a29ac580"),new ObjectID("6033534ecf6b755cd330beda"),new ObjectID("60332e80cf6b755cd32e82b8"),new ObjectID("602f7b7fcf6b7508a2a92069"),new ObjectID("602f5eb3cf6b7508a2a7298c"),new ObjectID("602b7af8cf6b7508a27468a9"),new ObjectID("602d0c14cf6b7508a28b80ea"),new ObjectID("602d0c60cf6b7508a28b85ef"),new ObjectID("6030e43ccf6b755cd326a9f8"),new ObjectID("6030872dcf6b755cd31fe1a2"),new ObjectID("60309979cf6b755cd3211f0f"),new ObjectID("60309474cf6b755cd320ced3"),new ObjectID("602d00e8cf6b7508a28a7bdc"),new ObjectID("6030a717cf6b755cd3220b46"),new ObjectID("6030ad98cf6b755cd3228d3b"),new ObjectID("60307f82cf6b755cd31f382b"),new ObjectID("6030816ecf6b755cd31f6cf6"),new ObjectID("602fc9f8cf6b7508a2ae0b9d"),new ObjectID("602faf1ccf6b7508a2acd3b0"),new ObjectID("602fad16cf6b7508a2acb9e8"),new ObjectID("602fb3dacf6b7508a2ad0049"),new ObjectID("602f5d7ccf6b7508a2a70cb8"),new ObjectID("602f867dcf6b7508a2a9c88c"),new ObjectID("602b7f94cf6b7508a274f5cf"),new ObjectID("6023d87fcf6b756955bb534c"),new ObjectID("602e1479cf6b7508a297a1ab"),new ObjectID("602f6673cf6b7508a2a7f959"),new ObjectID("602f2b56cf6b7508a2a2f3f0"),new ObjectID("602de495cf6b7508a2918a4f")]}}},*/
                {
                    "$project":{
                        "indvReportFiles":
                            {
                                $filter: {
                                    input: "$indvReportFiles",
                                    as: "indvReport",
                                    cond: { $and : filter
                                    }
                                }
                            }
                    }
                },
                {
                    "$project":{
                        CreditScore:{"$toInt":{$arrayElemAt:[{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.scores.score.scoreValue", -1 ]},0]}},
                        resOverdueAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.overdueAmt", -1 ]},
                        indvOverdueAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.overdueAmt", -1 ]},
                        grpOverdueAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.overdueAmt", -1 ]},
                        resCurrentBal:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.currentBal", -1 ]},
                        indvCurrentBal:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.currentBal", -1 ]},
                        grpCurrentBal:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.currentBal", -1 ]},
                        NumberOfAccounts:{$sum:[{$size:{$ifNull:[{$arrayElemAt:["$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response", -1 ]},[]]}},{$size:{$ifNull:[{$arrayElemAt:["$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse", -1 ]},[]]}},{$size:{$ifNull:[{$arrayElemAt:["$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse", -1 ]},[]]}}]},
                        NumberOfEnquiries:{$size:{ $ifNull: [ {$arrayElemAt:["$indvReportFiles.indvReportFile.indvReports.indvReport.inquiryHistory.history",-1]}, [] ] }},
                        HigherSanctionAmount: {$max:[{$max:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.disbursedAmt", -1 ]}},{$max:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.disbursedAmt", -1 ]}},{$max:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.disbursedAmt", -1 ]}}]},
                        OverdueBalance:{$sum:[{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.overdueAmt", -1 ]}},{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.overdueAmt", -1 ]}},{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.overdueAmt", -1 ]}}]},
                        CurrentBalance:{$sum:[{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.currentBal", -1 ]}},{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.currentBal", -1 ]}},{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.currentBal", -1 ]}}]},
                        resMaxDPD:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.maxDPD", -1 ]},
                        resNoEMI:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.noEMI", -1 ]},
                        resNoOfDPD:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.noOfDPD", -1 ]},
                        EnquiryDate:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.inquiryHistory.history.inquiryDate", -1 ]},
                        EnquiryAmount:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.inquiryHistory.history.amount", -1 ]},
                        RiskDesc:{$arrayElemAt:[{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.scores.score.scoreComments", -1 ]},0]},
                        resOverdueStatus:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.accountStatus", -1 ]},
                        indvOverdueStatus:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.status", -1 ]},
                        grpOverdueStatus:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.status", -1 ]},
                        resLoanStartDate:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.disbursedDate", -1 ]},
                        indvLoanStartDate:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.disbursedDt", -1 ]},
                        grpLoanStartDate:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.disbursedDt", -1 ]},
                        resLoanAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.disbursedAmt", -1 ]},
                        indvLoanAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.disbursedAmt", -1 ]},
                        grpLoanAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.disbursedAmt", -1 ]},
                        Total_other_EMI:{$sum:[{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.summary.totalOtherInstallmentAmount", -1 ]}},{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.summary.totalOtherInstallmentAmount", -1 ]}}]},
                        indvMaxDPD:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.maxDPD", -1 ]},
                        indvNoEMI:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.noEMI", -1 ]},
                        indvNoOfDPD:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.noOfDPD", -1 ]},
                        grpMaxDPD:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.maxDPD", -1 ]},
                        grpNoEMI:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.noEMI", -1 ]},
                        grpNoOfDPD:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.noOfDPD", -1 ]},

                        newAccountsInLastSixMonths:{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.accountsSummary.derivedAttributes.newAccountsInLastSixMonths", -1 ]}},
                        newDelinqAccountInLastSixMonths:{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.accountsSummary.derivedAttributes.newDelinqAccountInLastSixMonths", -1 ]}},
                        lengthOfCreditHistoryYear:{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.accountsSummary.derivedAttributes.lengthOfCreditHistoryYear", -1 ]}},
                        lengthOfCreditHistoryMonth:{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.accountsSummary.derivedAttributes.lengthOfCreditHistoryMonth", -1 ]}},
                        averageAccountAgeYear:{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.accountsSummary.derivedAttributes.averageAccountAgeYear", -1 ]}},
                        averageAccountAgeMonth:{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.accountsSummary.derivedAttributes.averageAccountAgeMonth", -1 ]}},
                        enquiriesInLastSixMonths:{$toInt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.accountsSummary.derivedAttributes.inquiriesInLastSixMonths", -1 ]}},
                        loanEnquiryPurpose:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.inquiryHistory.history.purpose", -1 ]},
                        emailVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.emailVariations.variation.value", -1 ]},
                        drivingLicenseVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.drivingLicenseVariations.variation.value", -1 ]},
                        nameVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.nameVariations.variation.value", -1 ]},
                        voterIdVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.voterIdVariations.variation.value", -1 ]},
                        addressVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.addressVariations.variation.value", -1 ]},
                        dateOfBirthVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.dateOfBirthVariations.variation.value", -1 ]},
                        rationCardVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.rationCardVariations.variation.value", -1 ]},
                        passportVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.passportVariations.variation.value", -1 ]},
                        phoneNumberVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.phoneNumberVariations.variation.value", -1 ]},
                        panVariations:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.personalInfoVariation.panVariations.variation.value", -1 ]},
                        indvAcctType:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.acctType", -1 ]},
                        indvWriteOffAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.writeOffAmt", -1 ]},
                        indvCombinedPaymentHistory:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.combinedPaymentHistory", -1 ]},
                        grpAcctType:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.acctType", -1 ]},
                        grpWriteOffAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.writeOffAmt", -1 ]},
                        grpCombinedPaymentHistory:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.combinedPaymentHistory", -1 ]},
                        resAcctType:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.acctType", -1 ]},
                        resOwnershipInd:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.ownershipInd", -1 ]},
                        resWriteOffAmt:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.writeOffAmt", -1 ]},
                        resCombinedPaymentHistory:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.combinedPaymentHistory", -1 ]},

                        resStatus:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.accountStatus", -1 ]},
                        indvStatus:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.status", -1 ]},
                        grpStatus:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.status", -1 ]},
                        indvDPDList:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.maxLoanList", -1 ]},
                        resDPDList:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.maxLoanList", -1 ]},
                        grpDPDList:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.maxLoanList", -1 ]}
                    }
                },
                {
                    "$project":{
                        "resOverdueAmt1":
                            {
                                $filter: {
                                    input: "$resOverdueAmt",
                                    as: "resOverdueAmt",
                                    cond: { $and:[
                                            {$gt: [ "$$resOverdueAmt", 0 ] }
                                        ]
                                    }
                                }
                            },
                        "indvOverdueAmt1":
                            {
                                $filter: {
                                    input: "$indvOverdueAmt",
                                    as: "indvOverdueAmt",
                                    cond: { $and:[
                                            {$gt: [ "$$indvOverdueAmt", 0 ] }
                                        ]
                                    }
                                }
                            },
                        "grpOverdueAmt1":
                            {
                                $filter: {
                                    input: "$grpOverdueAmt",
                                    as: "grpOverdueAmt",
                                    cond: { $and:[
                                            {$gt: [ "$$grpOverdueAmt", 0 ] }
                                        ]
                                    }
                                }
                            },
                        "resCurrentBal":
                            {
                                $filter: {
                                    input: "$resCurrentBal",
                                    as: "resCurrentBal",
                                    cond: { $and:[
                                            {$eq: [ "$$resCurrentBal", 0 ] }
                                        ]
                                    }
                                }
                            },
                        "indvCurrentBal":
                            {
                                $filter: {
                                    input: "$indvCurrentBal",
                                    as: "indvCurrentBal",
                                    cond: { $and:[
                                            {$eq: [ "$$indvCurrentBal", 0 ] }
                                        ]
                                    }
                                }
                            },
                        "grpCurrentBal":
                            {
                                $filter: {
                                    input: "$grpCurrentBal",
                                    as: "grpCurrentBal",
                                    cond: { $and:[
                                            {$eq: [ "$$grpCurrentBal", 0 ] }
                                        ]
                                    }
                                }
                            },
                        "resOverdueStatus":
                            {
                                $filter: {
                                    input: "$resOverdueStatus",
                                    as: "resOverdueStatus",
                                    cond: { $and:[
                                            {$eq: [ "$$resOverdueStatus", "Active" ] }
                                        ]
                                    }
                                }
                            },
                        "indvOverdueStatus":
                            {
                                $filter: {
                                    input: "$indvOverdueStatus",
                                    as: "indvOverdueStatus",
                                    cond: { $and:[
                                            {$eq: [ "$$indvOverdueStatus", "ACTIVE" ] }
                                        ]
                                    }
                                }
                            },
                        "grpOverdueStatus":
                            {
                                $filter: {
                                    input: "$grpOverdueStatus",
                                    as: "grpOverdueStatus",
                                    cond: { $and:[
                                            {$eq: [ "$$grpOverdueStatus", "ACTIVE" ] }
                                        ]
                                    }
                                }
                            },
                        NumberOfEnquiries:"$NumberOfEnquiries",
                        resDisbursedAmt:"$resDisbursedAmt",
                        indvDisbursedAmt:"$indvDisbursedAmt",
                        NumberOfAccounts:"$NumberOfAccounts",
                        grpDisbursedAmt:"$grpDisbursedAmt",
                        HigherSanctionAmount:"$HigherSanctionAmount",
                        OverdueBalance:"$OverdueBalance",
                        CurrentBalance:"$CurrentBalance",
                        resMaxDPD:"$resMaxDPD",
                        resNoEMI:"$resNoEMI",
                        resNoOfDPD:"$resNoOfDPD",
                        EnquiryDate:"$EnquiryDate",
                        EnquiryAmount:"$EnquiryAmount",
                        CreditScore:"$CreditScore",
                        RiskDesc:"$RiskDesc",
                        resLoanStartDate:"$resLoanStartDate",
                        indvLoanStartDate:"$indvLoanStartDate",
                        grpLoanStartDate:"$grpLoanStartDate",
                        resLoanAmt:"$resLoanAmt",
                        indvLoanAmt:"$indvLoanAmt",
                        grpLoanAmt:"$grpLoanAmt",
                        resOverdueAmt:"$resOverdueAmt",
                        indvOverdueAmt:"$indvOverdueAmt",
                        grpOverdueAmt:"$grpOverdueAmt",
                        Total_other_EMI:"$Total_other_EMI",
                        indvMaxDPD:"$indvMaxDPD",
                        indvNoEMI:"$indvNoEMI",
                        indvNoOfDPD:"$indvNoOfDPD",
                        grpMaxDPD:"$grpMaxDPD",
                        grpNoEMI:"$grpNoEMI",
                        grpNoOfDPD:"$grpNoOfDPD",

                        newAccountsInLastSixMonths: "$newAccountsInLastSixMonths",
                        newDelinqAccountInLastSixMonths:"$newDelinqAccountInLastSixMonths",
                        lengthOfCreditHistoryYear:"$lengthOfCreditHistoryYear",
                        lengthOfCreditHistoryMonth:"$lengthOfCreditHistoryMonth",
                        averageAccountAgeYear:"$averageAccountAgeYear",
                        averageAccountAgeMonth:"$averageAccountAgeMonth",
                        enquiriesInLastSixMonths:"$enquiriesInLastSixMonths",
                        loanEnquiryPurpose:"$loanEnquiryPurpose",
                        emailVariations:"$emailVariations",
                        drivingLicenseVariations:"$drivingLicenseVariations",
                        nameVariations:"$nameVariations",
                        voterIdVariations:"$voterIdVariations",
                        addressVariations:"$addressVariations",
                        dateOfBirthVariations:"$dateOfBirthVariations",
                        rationCardVariations:"$rationCardVariations",
                        passportVariations:"$passportVariations",
                        phoneNumberVariations:"$phoneNumberVariations",
                        panVariations:"$panVariations",
                        indvAcctType:"$indvAcctType",
                        indvWriteOffAmt:"$indvWriteOffAmt",
                        indvCombinedPaymentHistory:"$indvCombinedPaymentHistory",
                        grpAcctType:"$grpAcctType",
                        grpWriteOffAmt:"$grpWriteOffAmt",
                        grpCombinedPaymentHistory:"$grpCombinedPaymentHistory",
                        resAcctType:"$resAcctType",
                        resOwnershipInd:"$resOwnershipInd",
                        resWriteOffAmt:"$resWriteOffAmt",
                        resCombinedPaymentHistory:"$resCombinedPaymentHistory",

                        resStatus:"$resStatus",
                        indvStatus:"$indvStatus",
                        grpStatus:"$grpStatus",
                        resDPDList:"$resDPDList",
                        indvDPDList:"$indvDPDList",
                        grpDPDList:"$grpDPDList"
                    }
                },
                {
                    "$project":{
                        CreditScore:"$CreditScore",
                        OverdueAccounts:{"$sum":[{"$size":{ $ifNull: [ "$resOverdueAmt1", [] ] }},{"$size":{ $ifNull: [ "$indvOverdueAmt1", [] ] }},{"$size":{ $ifNull: [ "$grpOverdueAmt1", [] ] }}]},
                        ZeroBalanceAccounts:{"$sum":[{"$size":{ $ifNull: [ "$resCurrentBal", [] ] }},{"$size":{ $ifNull: [ "$indvCurrentBal", [] ] }},{"$size":{ $ifNull: [ "$grpCurrentBal", [] ] }}]},
                        NumberOfAccounts:"$NumberOfAccounts",
                        NumberOfEnquiries:"$NumberOfEnquiries",
                        resDisbursedAmt:"$resDisbursedAmt",
                        indvDisbursedAmt:"$indvDisbursedAmt",
                        grpDisbursedAmt:"$grpDisbursedAmt",
                        HigherSanctionAmount:"$HigherSanctionAmount",
                        OverdueBalance:"$OverdueBalance",
                        CurrentBalance:"$CurrentBalance",
                        resMaxDPD:"$resMaxDPD",
                        resNoEMI:"$resNoEMI",
                        resNoOfDPD:"$resNoOfDPD",
                        EnquiryDate:"$EnquiryDate",
                        EnquiryAmount:"$EnquiryAmount",
                        ActiveAccounts:{"$sum":[{"$size":{ $ifNull: [ "$resOverdueStatus", [] ] }},{"$size":{ $ifNull: [ "$indvOverdueStatus", [] ] }},{"$size":{ $ifNull: [ "$grpOverdueStatus", [] ] }}]},
                        RiskDesc:"$RiskDesc",
                        resLoanStartDate:"$resLoanStartDate",
                        indvLoanStartDate:"$indvLoanStartDate",
                        grpLoanStartDate:"$grpLoanStartDate",
                        resLoanAmt:"$resLoanAmt",
                        indvLoanAmt:"$indvLoanAmt",
                        grpLoanAmt:"$grpLoanAmt",
                        resOverdueAmt:"$resOverdueAmt",
                        indvOverdueAmt:"$indvOverdueAmt",
                        grpOverdueAmt:"$grpOverdueAmt",
                        Total_other_EMI:"$Total_other_EMI",
                        indvMaxDPD:"$indvMaxDPD",
                        indvNoEMI:"$indvNoEMI",
                        indvNoOfDPD:"$indvNoOfDPD",
                        grpMaxDPD:"$grpMaxDPD",
                        grpNoEMI:"$grpNoEMI",
                        grpNoOfDPD:"$grpNoOfDPD",

                        newAccountsInLastSixMonths: "$newAccountsInLastSixMonths",
                        newDelinqAccountInLastSixMonths:"$newDelinqAccountInLastSixMonths",
                        lengthOfCreditHistoryYear:"$lengthOfCreditHistoryYear",
                        lengthOfCreditHistoryMonth:"$lengthOfCreditHistoryMonth",
                        averageAccountAgeYear:"$averageAccountAgeYear",
                        averageAccountAgeMonth:"$averageAccountAgeMonth",
                        enquiriesInLastSixMonths:"$enquiriesInLastSixMonths",
                        loanEnquiryPurpose:"$loanEnquiryPurpose",
                        emailVariations:"$emailVariations",
                        drivingLicenseVariations:"$drivingLicenseVariations",
                        nameVariations:"$nameVariations",
                        voterIdVariations:"$voterIdVariations",
                        addressVariations:"$addressVariations",
                        dateOfBirthVariations:"$dateOfBirthVariations",
                        rationCardVariations:"$rationCardVariations",
                        passportVariations:"$passportVariations",
                        phoneNumberVariations:"$phoneNumberVariations",
                        panVariations:"$panVariations",
                        indvAcctType:"$indvAcctType",
                        indvWriteOffAmt:"$indvWriteOffAmt",
                        indvCombinedPaymentHistory:"$indvCombinedPaymentHistory",
                        grpAcctType:"$grpAcctType",
                        grpWriteOffAmt:"$grpWriteOffAmt",
                        grpCombinedPaymentHistory:"$grpCombinedPaymentHistory",
                        resAcctType:"$resAcctType",
                        resOwnershipInd:"$resOwnershipInd",
                        resWriteOffAmt:"$resWriteOffAmt",
                        resCombinedPaymentHistory:"$resCombinedPaymentHistory",

                        resStatus:"$resStatus",
                        indvStatus:"$indvStatus",
                        grpStatus:"$grpStatus",
                        resDPDList:"$resDPDList",
                        indvDPDList:"$indvDPDList",
                        grpDPDList:"$grpDPDList"
                    }
                }
            ]);

            try {
                await cursor.forEach(async function (value) {
                    pushCRIFValue(value,filterType);
                });
                resolve();
            }catch (e) {
                console.error(e);
                reject();
            }

        });

    });
}
var finalList = [];
var logStream = fs.createWriteStream('/home/arunchandrapanday/Customer_Journey_MSME.txt', {flags: 'a'});
var zero = 0;
var fivehund = 0
var fiveTo600 = 0
var six00to650 = 0
var six50to700 = 0
var seven00to750 = 0
var seven50to800 = 0
var gt800 = 0
var NA = 0
var x = 0
var resAcctType="";
var indvAcctType = "";
var grpAcctType = "";
function formatDate(date) {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2) month = '0' + month;
    if (day.length < 2) day = '0' + day;

    return [year, month, day].join('-');
}
try {
    Promise.all([getCreditReport(),getCustLoan(),getProspects(),getCRIFData(),getCRIF2(),getCRIFData("spouse"),getCRIF2("spouse")]).then( () => {
        //console.log(creditArray);
        //console.log(cust_loan);
        //console.log(prospec);
        prospec.forEach( async prospectsObj =>{
            try{
            }
            catch (error){
                console.log(prospectsObj["_id"])
                console.error(error)
            }

            let creditObj = creditArray[prospectsObj["_id"]];
            let loanObj = cust_loan[prospectsObj["_id"]];
            let crifObj = crif[prospectsObj["_id"]];
            let crif2Obj = crif2[prospectsObj["_id"]];
            let s_crifObj = s_crif[prospectsObj["_id"]];
            let s_crif2Obj = s_crif2[prospectsObj["_id"]];
            prospectsObj['density'] = 0;
            prospectsObj['distance'] = 0;//new code
            prospectsObj['merchant_base'] = 0;//new code
            let item = ["res","indv","grp"]
            let surveyArray = ["hasTelevision","hasRefrigerator","hasTwoWheeler","hasFourWheeler","costOfMobile","hasToilet",
                "hasKitchen","fuelType","numOfRooms","canArrangeFunds","sourceOfFund","businessLocation","hasBeenHospitalised",
                "monthylFamilySavings","hasInsurance","hasInvesments","lastInvestmentTime"];
            let identifierTypeArray = ["ID_PROOF_RATION_CARD","ID_PROOF_VOTER_ID","ID_PROOF_PASSPORT","ID_PROOF_AADHAAR","ID_PROOF_PAN"];
            identifierTypeArray.forEach(function (entry){
                prospectsObj[entry] = "";
            })

            for(let i=0;i<prospectsObj["identifierType"].length;i++){
                let identifierType = prospectsObj["identifierType"][i];
                let identifierNumber = prospectsObj["identifierNumber"][i];
                identifierTypeArray.forEach(function (entry){
                    if(identifierType === entry){
                        prospectsObj[entry] = md5(identifierNumber);
                    }
                });
            }

            surveyArray.forEach(function (entry){
                prospectsObj[entry] = "";
            })
            if(prospectsObj["catalogFieldKey"] !== undefined){
                for(let i=0;i<prospectsObj["catalogFieldKey"].length;i++){
                    let catalogKey = prospectsObj["catalogKey"][i];
                    if(catalogKey === "nonAgriSourceIncome"){
                        ["managedBy","sourceOfIncome","averageDailySales","profitMargin","netMonthlyIncome"].forEach(function (key){
                            surveyArray.push(key);
                        });
                    }
                    if(catalogKey === "agriSourceIncome"){
                        ["managedBy","sourceOfIncome","unitCd","totalOuputDaily","netAnnualIncome"].forEach(function (key){
                            surveyArray.push(key);
                        });
                    }
                    for(let j=0;j<prospectsObj["catalogFieldKey"][i].length;j++){
                        for(let k=0;k<prospectsObj["catalogFieldKey"][i][j].length;k++){
                            let catalogFieldKey = prospectsObj["catalogFieldKey"][i][j][k];
                            let catalogFieldValue = prospectsObj["catalogFieldValue"][i][j][k];
                            surveyArray.forEach(function (entry){
                                if(catalogFieldKey === entry){
                                    if(catalogKey === "nonAgriSourceIncome"){
                                        prospectsObj["nonAgri_"+entry] = catalogFieldValue;
                                    }else if(catalogKey === "agriSourceIncome"){
                                        prospectsObj["agri_"+entry] = catalogFieldValue;
                                    }else{
                                        prospectsObj[entry] = catalogFieldValue;
                                    }
                                }
                            });

                        }

                    }

                }
                delete prospectsObj["catalogKey"];
                delete prospectsObj["catalogFieldKey"];
                delete prospectsObj["catalogFieldValue"];
            }

            let crifObjArr = [];
            if(crifObj !== undefined){
                crifObjArr.push(crifObj);
                crifObj["typeOfObj"] = "b_";
            }

            if(s_crifObj !== undefined){
                crifObjArr.push(s_crifObj);
                s_crifObj["typeOfObj"] = "s_";
            }
            crifObjArr.forEach(function (crif){
                let pre= crif["typeOfObj"] === "s_" ? "s_" : "";
                if(typeof crif == "object" && crif ) {
                    let lastEnquiryDate = new Date(-8640000000000000);
                    let idx ;
                    let lastLoanDate = new Date(-8640000000000000);
                    item.forEach(function(entry){
                        if(crif[pre+entry+"LoanStartDate"] !== undefined ) {
                            for (let i = 0; i < crif[pre+entry+"LoanStartDate"].length; i++) {
                                let strLoanStartDate = crif[pre+entry+"LoanStartDate"][i];
                                let dateLoanStartDate = new Date(strLoanStartDate.split("-")[2], strLoanStartDate.split("-")[1] - 1, strLoanStartDate.split("-")[0])
                                if (lastLoanDate < dateLoanStartDate) {
                                    lastLoanDate = dateLoanStartDate;
                                }
                            }
                        }
                    });

                    if(lastLoanDate > new Date(-8640000000000000)){
                        lastLoanDate.setHours(lastLoanDate.getHours() + 5);
                        lastLoanDate.setMinutes(lastLoanDate.getMinutes() + 30);
                        crif[pre+"LastLoandate"] = lastLoanDate;

                    }


                    if(crif[pre+"EnquiryDate"] !== undefined ) {
                        for (let i = 0; i < crif[pre+"EnquiryDate"].length; i++) {
                            if (lastEnquiryDate < crif[pre+"EnquiryDate"][i]) {
                                lastEnquiryDate = crif[pre+"EnquiryDate"][i];
                                idx = i
                            }
                        }
                    }


                    if( idx !== undefined){
                        crif[pre+"LastEnquiryAmount"] = crif[pre+"EnquiryAmount"][idx];
                        //console.log(crifObj)
                    }

                    if(lastEnquiryDate > new Date(-8640000000000000)){
                        crif[pre+"LastEnquiryDate"] = lastEnquiryDate;
                    }

                    let emiPaidMonthly = {};
                    let noDPDMonthly = {};
                    let dPDMonthlyArr = {};
                    let monthlyActiveLoan = {};
                    item.forEach(function(entry){
                        if(crif[pre+entry+"CombinedPaymentHistory"] !== undefined ) {
                            for(let i = 0; i < crif[pre+entry+"CombinedPaymentHistory"].length ; i++){
                                let loanAmount = crif[pre+entry+"LoanAmt"][i];
                                let acctType = crif[pre+entry+"AcctType"][i].replace( /\s\s+/g, ' ' );
                                let interestRate = emiEstimationConst.variable[acctType]["IRR"];
                                let tenure = emiEstimationConst.variable[acctType]["Tenure"];
                                let emi = pmt(interestRate / 1200, tenure, -loanAmount );
                                let combinedPaymentHistory = crif[pre+entry+"CombinedPaymentHistory"][i].split("|");
                                for(let j = 0;j<combinedPaymentHistory.length;j++){
                                    let emiMonth = combinedPaymentHistory[j].split(",")[0];
                                    if(emiPaidMonthly[emiMonth] !== undefined && emiPaidMonthly[emiMonth] !== null && emiPaidMonthly[emiMonth] !== ""){
                                        emiPaidMonthly[emiMonth] += emi;
                                        monthlyActiveLoan[emiMonth] += 1;
                                    }else{
                                        emiPaidMonthly[emiMonth] = emi;
                                        monthlyActiveLoan[emiMonth] = 1;
                                    }
                                    if(crif[pre+entry+"DPDList"][i]){
                                        let dpdEntry = crif[pre+entry+"DPDList"][i][j];
                                        if(dpdEntry > 0 ){
                                            if(noDPDMonthly[emiMonth] === undefined)
                                                noDPDMonthly[emiMonth] = 0;
                                            noDPDMonthly[emiMonth] += 1
                                            if(dPDMonthlyArr[emiMonth] === undefined)
                                                dPDMonthlyArr[emiMonth] = [];
                                            dPDMonthlyArr[emiMonth].push(dpdEntry);
                                        }
                                    }
                                }
                            }
                        }
                        delete crif[pre+entry+"CombinedPaymentHistory"];
                    });
                    delete emiPaidMonthly[""];
                    delete noDPDMonthly[""];
                    delete dPDMonthlyArr[""];
                    delete monthlyActiveLoan[""];
                    crif[pre+"emiPaidMonthly"]=emiPaidMonthly;

                    crif[pre+"noDPDMonthly"] = noDPDMonthly;
                    crif[pre+"dPDMonthlyArr"] = dPDMonthlyArr;
                    crif[pre+"monthlyActiveLoan"] = monthlyActiveLoan;

                    crif[pre+"activeLoanDisburseAmt"] = 0;
                    item.forEach(function (entry){
                        let disbursedAmtArr = crif[pre+entry+"LoanAmt"];
                        let statusArr = crif[pre+entry+"Status"]
                        if(disbursedAmtArr !== undefined ) {
                            for(let i = 0; i < disbursedAmtArr.length; i++){
                                if(statusArr[i].toUpperCase() === "ACTIVE"){//new code
                                    crif[pre+"activeLoanDisburseAmt"] += disbursedAmtArr[i];
                                }
                            }
                        }
                    });



                }
                delete crif["typeOfObj"];
            });

            if(typeof creditObj =="object" && creditObj) {
                if (creditObj['statusCd'] === 'REJECTED' || creditObj['statusCd'] === 'ELIGIBILITY_REJECT' || creditObj['statusCd'] === 'CREDIT_REJECT') {
                    creditObj["CreditStatus"] = "auto_reject"
                } else if (creditObj['statusCd'] === 'PENDING_LOAN_APPLICATION') {
                    creditObj["CreditStatus"] = "auto_accept"
                } else if (creditObj['statusCd'] === 'PENDING_DEVIATION_REVIEW') {
                    creditObj["CreditStatus"] = "deviation_flow"
                }
                if( ['PRGTYP0004','PRGTYP0009','PRGTYP0007'].includes(creditObj['SchemeName'])){
                    creditObj['SchemeName'] = "PRGTYP0005"
                } else if( ['PRGTYP0006','PRGTYP0008'].includes(creditObj['SchemeName'])){
                    creditObj['SchemeName'] = "PRGTYP0003"
                }
            }

            if(typeof loanObj =="object" && loanObj) {
                if (loanObj['size2'] === 0 && loanObj['size1'] === 1) {
                    loanObj["LoanStatus"] = "deviation_reject";
                } else if (['LOAN_DISBURSEMENT_INITIATED', 'LOAN_BOOKING_INTITIATED'].includes(loanObj['stages'])) {
                    loanObj["LoanStatus"] = "final_approval";
                } else if (loanObj['size2'] > 0 && loanObj['size1'] === 1) {
                    loanObj["LoanStatus"] = "deviation_accept";
                } else if (loanObj['kyc'] >= 1 && loanObj['statusCd'] === 'LOAN_REJECTED') {
                    loanObj["LoanStatus"] = "eye_review_reject";
                } else if (loanObj['kyc'] >= 1) {
                    loanObj["LoanStatus"] = "eye_review";
                }

                if(prospectsObj["locationLatitude"]){
                    let prospectslat = prospectsObj["locationLatitude"];
                    let prospectsLong = prospectsObj["locationLongitude"];
                    let prospectsPoint = new GeoPoint(prospectslat, prospectsLong);
                    if(loanObj['assignedUserId']){
                        let customer_profiles= await findAll('kyros_origination','customer_profiles',{"merchantUserId":loanObj['assignedUserId'],"state":901});
                        customer_profiles.data.forEach((customer_profile) => {
                            if(customer_profile['location'] && customer_profile['location']['coordinates']){
                                prospectsObj['merchant_base']++;//new code
                                let custProfileLat = customer_profile['location']['coordinates'][1];
                                let custProfileLong = customer_profile['location']['coordinates'][0];
                                if(custProfileLat && custProfileLong){
                                    let customerGeoPoint = new GeoPoint(custProfileLat,custProfileLong)
                                    if((prospectsPoint.distanceTo(customerGeoPoint, true)*1.4) < 2) prospectsObj['density']++;
                                }
                            }
                        });
                        //new code
                        let statement = "select setting_name,setting_value from kyros_admin.user_setting_tmp where setting_group = 'GEO_LOCATION' and status_cd = 901 and user_id = '"+loanObj['assignedUserId']+"';";
                        let merchant_rows = await statementExecutorAdminDB(statement);
                        let merchantLat = 0;
                        let merchantLong = 0;
                        _.forEach(merchant_rows,(merchant_row)=>{
                            if(merchant_row.setting_name === "LATITUDE"){
                                merchantLat = parseFloat(merchant_row.setting_value);
                            }
                            else if(merchant_row.setting_name === "LONGITUDE"){
                                merchantLong = parseFloat(merchant_row.setting_value);
                            }
                        });
                        let merchantGeoPoint;
                        if(merchantLat !==0 && merchantLong !==0){
                            console.log("merchantLat:"+merchantLat+"prospectsObj:"+prospectsObj["_id"])
                            merchantGeoPoint = new GeoPoint(merchantLat,merchantLong)
                        }

                        if(merchantGeoPoint){
                            prospectsObj['distance'] = prospectsPoint.distanceTo(merchantGeoPoint, true);
                        }
                        //new code

                    }
                    delete prospectsObj["locationLatitude"];
                    delete prospectsObj["locationLongitude"];
                }

            }
            delete prospectsObj["identifierType"];
            delete prospectsObj["identifierNumber"];


            //console.log(JSON.stringify(Object.assign({},prospectsObj,creditObj,loanObj,crifObj,crif2Obj,s_crifObj,s_crif2Obj,)))
            let xyz = JSON.stringify(Object.assign({},prospectsObj,creditObj,loanObj,crifObj,crif2Obj,s_crifObj,s_crif2Obj));
            /*if (loanObj && loanObj["FinanceId"] && loanObj["FinanceId"] !== "undefined" && Date.parse(loanObj["disbursementDate"]) > 1546300800000 && Date.parse(loanObj["disbursementDate"]) < 1606780800000  ) {
                console.log(loanObj["FinanceId"]+","+crifObj["CreditScore"]);
               /!* if (crifObj || crifObj["CreditScore"] || crifObj["CreditScore"] == undefined || crifObj["CreditScore"] == null) {
                    NA++;
                }
                if (crifObj["CreditScore"] == 0 || crifObj["CreditScore"] == -1) {
                    zero++;
                } else if (crifObj["CreditScore"] <= 500) {
                    fivehund++;
                } else if (crifObj["CreditScore"] <= 600) {
                    fiveTo600++;
                } else if (crifObj["CreditScore"] <= 650) {
                    six00to650++;
                } else if (crifObj["CreditScore"] <= 700) {
                    six50to700++;
                } else if (crifObj["CreditScore"] <= 750) {
                    seven00to750++;
                } else if (crifObj["CreditScore"] <= 800) {
                    seven50to800++;
                } else if (crifObj["CreditScore"] > 800) {
                    gt800++;
                }*!/
            }*/
            logStream.write(xyz+"\n");


        });
        /* console.log("0 To -1,"+zero);
         console.log(" < 500,"+fivehund);
         console.log("501-600,"+fiveTo600);
         console.log("601-650,"+six00to650);
         console.log("651 - 700,"+six50to700);
         console.log("701 - 750,"+seven00to750);
         console.log("751 - 800,"+seven50to800);
         console.log("Gr than 800,"+gt800);
         console.log("NA,"+NA);*/
        process.exit;
    }).catch((error) => {
        //console.error(JSON.stringify(prospectsObj))
        //console.error(error);
        //res.status(400);
        res.send(error.message || error);
    });
} catch(error) {
    //console.error("Arun")
    // code to run if there are any problems
} finally {
}





