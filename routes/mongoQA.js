var MongoPool = require('./mongoDB');
const fs = require('fs');
const {ObjectID} = require('mongodb');

var prospec = [];
function pushPros(value) {
    prospec.push(value);
}

function getProspects(){

    return new Promise((resolve, reject) => {

        if(!MongoPool){
            reject(new Error('Mongo Connection is not Available'));
            return;
        }

        MongoPool.getInstance(async function (client) {
            /*const collection = client.db("kyros_origination").collection("customer_loan_applications");*/
            const collection = client.db("kyros_origination").collection("crif_credit_reports");
            // perform actions on the collection object

            /*let cursor = collection.aggregate([
                {
                    "$match":{
                        "$or":[
                            {
                                "legalForm":{
                                    "$exists":true,
                                    "$eq":"LEGAL_FORM_INDIVIDUAL"
                                }
                            },
                            {
                                "legalForm":{
                                    "$exists":false
                                }
                            }
                        ]
                    }
                },
                {
                    "$match":{
                        "$and":[
                            {
                                "creditRequirementCreationDate":{
                                    "$gte":new Date("2020-06-23T11:32:11.078Z")
                                }
                            }
                        ]
                    }
                },
                {
                    "$project":{
                        "statusCd":"$loanStatus",
                        "customerId":"$customerId",
                        "state":{
                            "$arrayElemAt":[
                                {
                                    "$split":[
                                        "$locationTag",
                                        "|"
                                    ]
                                },
                                0
                            ]
                        },
                        "district":{
                            "$arrayElemAt":[
                                {
                                    "$split":[
                                        "$locationTag",
                                        "|"
                                    ]
                                },
                                1
                            ]
                        },
                        "weekNumber":{
                            "$cond":{
                                "if":{
                                    "$lt":[
                                        {
                                            "$year":"$creditRequirementCreationDate"
                                        },
                                        {
                                            "$year":new Date("2020-10-21T11:32:11.078Z")
                                        }
                                    ]
                                },
                                "then":{
                                    "$floor":{
                                        "$divide":[
                                            {
                                                "$subtract":[
                                                    {
                                                        "$month":"$creditRequirementCreationDate"
                                                    },
                                                    12
                                                ]
                                            },
                                            1
                                        ]
                                    }
                                },
                                "else":{
                                    "$floor":{
                                        "$divide":[
                                            {
                                                "$month":"$creditRequirementCreationDate"
                                            },
                                            1
                                        ]
                                    }
                                }
                            }
                        },
                        "flag":{
                            "$filter":{
                                "input":"$stages",
                                "as":"stages",
                                "cond":{
                                    "$and":[
                                        {
                                            "$eq":[
                                                "$$stages.statusCd",
                                                "PENDING_DEVIATION_REVIEW"
                                            ]
                                        },
                                        {
                                            "$eq":[
                                                "$$stages.stageName",
                                                "DEVIATION_REVIEW_ASSIGNMENT"
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        "kyc":{
                            "$filter":{
                                "input":"$stages",
                                "as":"stages",
                                "cond":{
                                    "$and":[
                                        {
                                            "$eq":[
                                                "$$stages.stageName",
                                                "FINAL_APPROVAL"
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        "stages":{
                            "$slice":[
                                "$stages",
                                -1
                            ]
                        }
                    }
                },
                {
                    "$match":{
                        "$and":[
                            {
                                "weekNumber":{
                                    "$eq":7
                                }
                            }
                        ]
                    }
                },
                {
                    "$project":{
                        "statusCd":"$statusCd",
                        "customerId":"$customerId",
                        "weekNumber":"$weekNumber",
                        "size1":{
                            "$size":"$flag"
                        },
                        "kyc":{
                            "$size":"$kyc"
                        },
                        "stages":{
                            "$arrayElemAt":[
                                "$stages.stageName",
                                0
                            ]
                        }
                    }
                }
            ]);*/
            let cursor = collection.aggregate([
                /*{"$match":{"_id":new ObjectID("5dfb66d757e84010a0b68e6b")}},*/
                {
                    "$project":{
                        "indvReportFiles":
                            {
                                $filter: {
                                    input: "$indvReportFiles",
                                    as: "indvReport",
                                    cond: { $and : [
                                            {$eq: [ "$$indvReport.spouseReport", true ] },
                                            {$eq: [ "$$indvReport.status", 901 ] }
                                        ]
                                    }
                                    /*cond: { $and : [
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
                                        ]
                                    }*/
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
                        OverdueBalance:{$sum:[{$sum:{}},{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.overdueAmt", -1 ]}},{$sum:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.overdueAmt", -1 ]}}]},
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
                        grpNoOfDPD:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.noOfDPD", -1 ]}/*,
                        x:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.responses.response.loanDetails.currentBal", -1 ]},
                        y:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList.indvResponse.loanDetail.currentBal", -1 ]},
                        z:{$arrayElemAt: [ "$indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList.grpResponse.loanDetail.currentBal", -1 ]}*/
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
                        grpNoOfDPD:"$grpNoOfDPD"/*,
                        x:"$x",
                        y:"$y",
                        z:"$z"*/
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
                        grpNoOfDPD:"$grpNoOfDPD"/*,
                        x:"$x",
                        y:"$y",
                        z:"$z"*/
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



var finalList = [];
var logStream = fs.createWriteStream('/home/arunchandrapanday/Spouse_CRIF.txt', {flags: 'a'});
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
try {
    Promise.all([getProspects()]).then( () => {
        //console.log(creditArray);
        //console.log(cust_loan);
        //console.log(prospec);
        prospec.forEach( async prospectsObj =>{
            /*console.log(prospectsObj["statusCd"]+","+prospectsObj["customerId"]+","+prospectsObj["weekNumber"]+","+prospectsObj["size1"]+","+prospectsObj["kyc"]+","+prospectsObj["stages"]);*/
            //console.log(JSON.stringify(prospectsObj));
            logStream.write(JSON.stringify(prospectsObj)+"\n");
        });

    }).catch((error) => {
        console.error(error);
        res.status(400);
        res.send(error.message || error);
    });
} catch(error) {
    console.error("Arun")
    // code to run if there are any problems
} finally {
}





