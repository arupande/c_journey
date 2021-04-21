var MongoPool = require('./mongoDB');
const fs = require('fs');
const {ObjectID} = require('mongodb');

var crif = [];
var crif2 = {};
function pushCRIFValue(value) {
    crif.push(value) ;
}

function pushCRIF2(value) {
    crif2[value["_id"]] = value;
}

function getCRIFData(){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }

        MongoPool.getInstance(async function (client) {
            const collection = client.db("kyros_origination").collection("mock_crif_credit_reports");
            // perform actions on the collection object

            let cursor = collection.aggregate([
                //{"$match":{ "_id" : new ObjectID("5e11adcedb14e7555534df31")}},
                {
                    "$project":{
                        CustomerId:"$reportId",
                        CreditScore:{"$toInt":{$arrayElemAt:["$indvReportFile.indvReports.indvReport.scores.score.scoreValue",0]}},
                        resOverdueAmt:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.overdueAmt",
                        indvOverdueAmt:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.overdueAmt",
                        grpOverdueAmt:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.overdueAmt",
                        resCurrentBal:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.currentBal",
                        indvCurrentBal:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.currentBal",
                        grpCurrentBal:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.currentBal",
                        NumberOfAccounts:{$sum:[{$size:{$ifNull:["$indvReportFile.indvReports.indvReport.responses.response",[]]}},{$size:{$ifNull:["$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse",[]]}},{$size:{$ifNull:["$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse",[]]}}]},
                        NumberOfEnquiries:{$size:{ $ifNull: [ "$indvReportFile.indvReports.indvReport.inquiryHistory.history", [] ] }},
                        HigherSanctionAmount: {$max:[{$max:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.disbursedAmt"},{$max:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.disbursedAmt"},{$max:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.disbursedAmt"}]},
                        OverdueBalance:{$sum:[{$sum:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.overdueAmt"},{$sum:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.overdueAmt"},{$sum:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.overdueAmt"}]},
                        CurrentBalance:{$sum:[{$sum:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.currentBal"},{$sum:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.currentBal"},{$sum:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.currentBal"}]},
                        MaxDPD:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.maxDPD",
                        NoEMI:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.noEMI",
                        NoOfDPD:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.noOfDPD",
                        EnquiryDate:"$indvReportFile.indvReports.indvReport.inquiryHistory.history.inquiryDate",
                        EnquiryAmount:"$indvReportFile.indvReports.indvReport.inquiryHistory.history.amount",
                        RiskDesc:{$arrayElemAt:["$indvReportFile.indvReports.indvReport.scores.score.scoreComments",0]},
                        resOverdueStatus:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.accountStatus",
                        indvOverdueStatus:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.status",
                        grpOverdueStatus:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.status",
                        resLoanStartDate:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.disbursedDate",
                        indvLoanStartDate:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.disbursedDt",
                        grpLoanStartDate:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.disbursedDt",
                        resLoanAmt:"$indvReportFile.indvReports.indvReport.responses.response.loanDetails.disbursedAmt",
                        indvLoanAmt:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.disbursedAmt",
                        grpLoanAmt:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.disbursedAmt",
                        Total_other_EMI:{$sum:[{$toInt:
                                    "$indvReportFile.indvReports.indvReport.indvResponses.summary.totalOtherInstallmentAmount"},{$toInt:"$indvReportFile.indvReports.indvReport.grpResponses.summary.totalOtherInstallmentAmount"}]},
                        indvMaxDPD:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.maxDPD",
                        indvNoEMI:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.noEMI",
                        indvNoOfDPD:"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.noOfDPD",
                        grpMaxDPD:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.maxDPD",
                        grpNoEMI:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.noEMI",
                        grpNoOfDPD:"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.noOfDPD"
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
                        CustomerId:"$CustomerId",
                        NumberOfEnquiries:"$NumberOfEnquiries",
                        resDisbursedAmt:"$resDisbursedAmt",
                        indvDisbursedAmt:"$indvDisbursedAmt",
                        NumberOfAccounts:"$NumberOfAccounts",
                        grpDisbursedAmt:"$grpDisbursedAmt",
                        HigherSanctionAmount:"$HigherSanctionAmount",
                        OverdueBalance:"$OverdueBalance",
                        CurrentBalance:"$CurrentBalance",
                        MaxDPD:"$MaxDPD",
                        NoEMI:"$NoEMI",
                        NoOfDPD:"$NoOfDPD",
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
                        grpNoOfDPD:"$grpNoOfDPD"
                    }
                },
                {
                    "$project":{
                        CustomerId:"$CustomerId",
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
                        resMaxDPD:"$MaxDPD",
                        resNoEMI:"$NoEMI",
                        resNoOfDPD:"$NoOfDPD",
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
                        grpNoOfDPD:"$grpNoOfDPD"
                    }
                }
            ]);

            try {
                await cursor.forEach(async function (value) {
                    pushCRIFValue(value);
                });
                resolve();
            }catch (e) {
                console.error(e);
                reject();
            }

        });

    });
}

function getCRIF2(){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }

        MongoPool.getInstance(async function (client) {
            const collection = client.db("kyros_origination").collection("mock_crif_credit_reports");
            // perform actions on the collection object

            let cursor = collection.aggregate([
                    //{"$match":{"_id": new ObjectID("5dfb1f4857e84010a0b66765")}},
                    //{"$project":{"indvReportFiles":{$arrayElemAt: [ "$indvReportFiles", 0 ]}}},
                    {"$unwind":{"path":"$indvReportFile.indvReports.indvReport.responses.response","preserveNullAndEmptyArrays": true}},
                    {
                        "$group":{
                            "_id":{_id:"$_id","indv":"$indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse","grp":"$indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse"},
                            "resInstalment": {
                                "$push": {
                                    "$cond":[
                                        { $not: ["$indvReportFile.indvReports.indvReport.responses.response.loanDetails.noEMI"] },
                                        //{$ne : ["$indvReportFile.indvReports.indvReport.responses.response.loanDetails.maxDPD", undefined]},
                                        null,
                                        {"$ifNull":["$indvReportFile.indvReports.indvReport.responses.response.loanDetails.installmentAmt","NA"]}

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
                    pushCRIF2(value);
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
var logStream = fs.createWriteStream('/home/arun/CRIF_AllCloud.txt', {flags: 'a'});
var x = 0
try {
    Promise.all([getCRIFData(),getCRIF2()]).then( () => {
        //console.log(creditArray);
        //console.log(cust_loan);
        //console.log(prospec);
        crif.forEach( async crifObj =>{
            let crif2Obj = crif2[crifObj["_id"]]
            if(typeof crifObj =="object" && crifObj ) {
                let LastEnquiryDate = new Date(-8640000000000000);
                let idx ;
                let LastEnquiryAmount;
                let LastLoandate = new Date(-8640000000000000);

                //console.log("Arun:"+crifObj["EnquiryDate"]+"_id:"+crifObj["_id"])
                if(crifObj["resLoanStartDate"] !== undefined ) {
                    for (var i = 0; i < crifObj["resLoanStartDate"].length; i++) {
                        let strResLoanStartDate = crifObj["resLoanStartDate"][i];
                        let dateResLoanStartDate = new Date(strResLoanStartDate.split("-")[2], strResLoanStartDate.split("-")[1] - 1, strResLoanStartDate.split("-")[0])
                        if (LastLoandate < dateResLoanStartDate) {
                            LastLoandate = dateResLoanStartDate;
                        }
                    }
                }

                if(crifObj["indvLoanStartDate"] !== undefined ) {
                    for (var i = 0; i < crifObj["indvLoanStartDate"].length; i++) {
                        let strResLoanStartDate = crifObj["indvLoanStartDate"][i];
                        let dateResLoanStartDate = new Date(strResLoanStartDate.split("-")[2], strResLoanStartDate.split("-")[1] - 1, strResLoanStartDate.split("-")[0])
                        if (LastLoandate < dateResLoanStartDate) {
                            LastLoandate = dateResLoanStartDate;
                        }
                    }
                }

                if(crifObj["grpLoanStartDate"] !== undefined ) {
                    for (var i = 0; i < crifObj["grpLoanStartDate"].length; i++) {
                        let strResLoanStartDate = crifObj["grpLoanStartDate"][i];
                        let dateResLoanStartDate = new Date(strResLoanStartDate.split("-")[2], strResLoanStartDate.split("-")[1] - 1, strResLoanStartDate.split("-")[0])
                        if (LastLoandate < dateResLoanStartDate) {
                            LastLoandate = dateResLoanStartDate;
                        }
                    }
                }

                if(LastLoandate > new Date(-8640000000000000)){
                    LastLoandate.setHours(LastLoandate.getHours() + 5);
                    LastLoandate.setMinutes(LastLoandate.getMinutes() + 30);
                    crifObj["LastLoandate"] = LastLoandate;
                    //console.log(crifObj)
                }

                //console.log("Arun:"+crifObj["EnquiryDate"]+"_id:"+crifObj["_id"])
                if(crifObj["EnquiryDate"] !== undefined ) {
                    for (var i = 0; i < crifObj["EnquiryDate"].length; i++) {
                        if (LastEnquiryDate < crifObj["EnquiryDate"][i]) {
                            LastEnquiryDate = crifObj["EnquiryDate"][i];
                            idx = i
                        }
                    }
                }

                if( idx !== undefined){
                    crifObj["LastEnquiryAmount"] = crifObj["EnquiryAmount"][idx];
                    //console.log(crifObj)
                }

                if(LastEnquiryDate > new Date(-8640000000000000)){
                    crifObj["LastEnquiryDate"] = LastEnquiryDate;
                    //console.log(crifObj)
                }

            }

            //console.log(JSON.stringify(Object.assign({},crifObj,crif2Obj)))
            let xyz = JSON.stringify(Object.assign({},crifObj,crif2Obj));
            console.log(x++);
            logStream.write(xyz+"\n");

        });
        process.exit;
    }).catch((error) => {
        console.erro(JSON.stringify(prospectsObj))
        console.error(error);
    });
} catch(error) {
    console.error("Arun")
    // code to run if there are any problems
} finally {
}
