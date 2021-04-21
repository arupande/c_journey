const {ObjectID} = require('mongodb');
var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');
let LastUpdatedDate='';
MongoClient.connect('mongodb://localhost:27017/kyros_origination',
    function(err, client) {

        if(err) throw(err);

        var cur = client.db("kyros_origination").collection('mock_crif_credit_reports').find({/*"_id":new ObjectID("5e36f0c7db14e77ac70f9efb")*/});
        var updates = 0;
        var curDone=false;
        LastUpdatedDate = moment.utc();
        cur.each(function(err, doc){
            if(err) throw err;
            if (doc != null){
                console.log(doc._id + "-- ");
                let indvRep = doc.indvReportFile


                //console.log(JSON.stringify(indvRep))

                //for(var i = 0; i < indvRep.length; i++  ){
                    let indvReportFile = doc.indvReportFile;
                    let res1 = indvReportFile.indvReports.indvReport.responses

                    if( typeof res1 == 'object' && res1) {
                        let res= res1["response"];
                        //console.log("res:"+JSON.stringify(res))
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let maxLoanList = [];
                            let noOfDPD = 0;
                            let combinedPaymentHistoryList = resLoan["loanDetails"]["combinedPaymentHistory"].split("|")
                            combinedPaymentHistoryList.forEach(combinedPaymentHistory=>{
                                if(combinedPaymentHistory !== "undefined" && combinedPaymentHistory !== ""){
                                    let dpd = parseInt(combinedPaymentHistory.split(",")[1].split("/")[0]
                                        .replace("DDD","0")
                                        .replace("XXX","0")
                                        .replace("DDD","0")
                                    );

                                    if(dpd > 0){
                                        noOfDPD++
                                    }
                                    maxLoanList.push(dpd)
                                    resLoan["loanDetails"]["maxLoanList"] = maxLoanList;
                                    resLoan["loanDetails"]["maxDPD"] = Math.max.apply(Math, maxLoanList);
                                    resLoan["loanDetails"]["noEMI"] = maxLoanList.length;
                                    resLoan["loanDetails"]["noOfDPD"] = noOfDPD;
                                }

                            })
                            let disbursedAmt = resLoan["loanDetails"]["disbursedAmt"]
                            if(resLoan["loanDetails"]["disbursedAmt"] !== undefined)
                                resLoan["loanDetails"]["disbursedAmt"] = parseInt(disbursedAmt.replace(/,/g, ''));
                            if(resLoan["loanDetails"] && resLoan["loanDetails"]["disbursedAmt"] !== undefined)
                                resLoan["loanDetails"]["disbursedAmt"] = 0
                            let overdueAmt = resLoan["loanDetails"]["overdueAmt"]
                            if(resLoan["loanDetails"]["overdueAmt"] !== undefined)
                                resLoan["loanDetails"]["overdueAmt"] = parseInt(overdueAmt.replace(/,/g, ''));
                            let writeOffAmt = resLoan["loanDetails"]["writeOffAmt"]
                            if(resLoan["loanDetails"]["writeOffAmt"] !== undefined)
                                resLoan["loanDetails"]["writeOffAmt"] = parseInt(writeOffAmt.replace(/,/g, ''));
                            let currentBal = resLoan["loanDetails"]["currentBal"]
                            if(resLoan["loanDetails"]["currentBal"] !== undefined)
                                resLoan["loanDetails"]["currentBal"] = parseInt(currentBal.replace(/,/g, ''));
                        }
                        //console.log("res:"+JSON.stringify(res))
                    }

                    let indv = indvReportFile.indvReports.indvReport.indvResponses.indvResponseList;
                    if( typeof indv == 'object' && indv) {
                        let res= indv["indvResponse"];
                        for (var j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let maxLoanList = [];
                            let noOfDPD = 0;
                            let combinedPaymentHistoryList = resLoan["loanDetail"]["combinedPaymentHistory"].split("|");
                            combinedPaymentHistoryList.forEach(combinedPaymentHistory=>{
                                if(combinedPaymentHistory !== undefined && combinedPaymentHistory !== ""){
                                    let dpd = parseInt(combinedPaymentHistory.split(",")[1]
                                        .replace("DDD","0")
                                        .replace("XXX","0")
                                        .replace("DDD","0")
                                    );
                                    //console.log(dpd)
                                    if(dpd !== undefined && dpd > 0){
                                        noOfDPD++;
                                    }

                                    if(maxLoanList != undefined){
                                        maxLoanList.push(dpd);
                                        resLoan["loanDetail"]["maxLoanList"] = maxLoanList;
                                        resLoan["loanDetail"]["maxDPD"] = Math.max.apply(Math, maxLoanList);
                                        resLoan["loanDetail"]["noEMI"] = maxLoanList.length;
                                        resLoan["loanDetail"]["noOfDPD"] = noOfDPD;
                                    }
                                }
                            });
                        }
                    }

                    if( typeof indv == 'object' && indv) {
                        let res = indv["indvResponse"];
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let disbursedAmt = resLoan["loanDetail"]["disbursedAmt"]
                            if (resLoan["loanDetail"]["disbursedAmt"] !== undefined)
                                resLoan["loanDetail"]["disbursedAmt"] = parseInt(disbursedAmt.replace(/,/g, ''));
                            let overdueAmt = resLoan["loanDetail"]["overdueAmt"]
                            if (resLoan["loanDetail"]["overdueAmt"] !== undefined)
                                resLoan["loanDetail"]["overdueAmt"] = parseInt(overdueAmt.replace(/,/g, ''));
                            let writeOffAmt = resLoan["loanDetail"]["writeOffAmt"]
                            if (resLoan["loanDetail"]["writeOffAmt"] !== undefined)
                                resLoan["loanDetail"]["writeOffAmt"] = parseInt(writeOffAmt.replace(/,/g, ''));
                            let currentBal = resLoan["loanDetail"]["currentBal"]
                            if (resLoan["loanDetail"]["currentBal"] !== undefined)
                                resLoan["loanDetail"]["currentBal"] = parseInt(currentBal.replace(/,/g, ''))
                        }

                    }

                    let grp = indvReportFile.indvReports.indvReport.grpResponses.grpResponseList;
                    if( typeof grp == 'object' && grp) {
                        let res = grp["grpResponse"];
                        for (var j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let maxLoanList = [];
                            let noOfDPD = 0;
                            let combinedPaymentHistoryList = resLoan["loanDetail"]["combinedPaymentHistory"].split("|");
                            combinedPaymentHistoryList.forEach(combinedPaymentHistory=>{
                                if(combinedPaymentHistory !== undefined && combinedPaymentHistory !== ""){
                                    let dpd = parseInt(combinedPaymentHistory.split(",")[1]
                                        .replace("DDD","0")
                                        .replace("XXX","0")
                                        .replace("DDD","0")
                                    );
                                    //console.log(dpd)
                                    if(dpd !== undefined && dpd > 0){
                                        noOfDPD++;
                                    }
                                    if(maxLoanList != undefined){
                                        maxLoanList.push(dpd);
                                        resLoan["loanDetail"]["maxLoanList"] = maxLoanList;
                                        resLoan["loanDetail"]["maxDPD"] = Math.max.apply(Math, maxLoanList);
                                        resLoan["loanDetail"]["noEMI"] = maxLoanList.length;
                                        resLoan["loanDetail"]["noOfDPD"] = noOfDPD;
                                    }
                                }
                            });
                        }
                    }
                    if( typeof grp == 'object' && grp) {
                        let res = grp["grpResponse"];
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let disbursedAmt = resLoan["loanDetail"]["disbursedAmt"]
                            if (resLoan["loanDetail"]["disbursedAmt"] !== undefined)
                                resLoan["loanDetail"]["disbursedAmt"] = parseInt(disbursedAmt.replace(/,/g, ''))
                            let overdueAmt = resLoan["loanDetail"]["overdueAmt"]
                            if (resLoan["loanDetail"]["overdueAmt"] !== undefined)
                                resLoan["loanDetail"]["overdueAmt"] = parseInt(overdueAmt.replace(/,/g, ''))
                            let writeOffAmt = resLoan["loanDetail"]["writeOffAmt"]
                            if (resLoan["loanDetail"]["writeOffAmt"] !== undefined)
                                resLoan["loanDetail"]["writeOffAmt"] = parseInt(writeOffAmt.replace(/,/g, ''))
                            let currentBal = resLoan["loanDetail"]["currentBal"]
                            if (resLoan["loanDetail"]["currentBal"] !== undefined)
                                resLoan["loanDetail"]["currentBal"] = parseInt(currentBal.replace(/,/g, ''))
                        }

                    }

                    let inqHist = indvReportFile.indvReports.indvReport.inquiryHistory
                    if( typeof inqHist == 'object' && inqHist) {
                        let res = inqHist["history"];
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let inquiryDate = resLoan["inquiryDate"].split("-")
                            if (resLoan["inquiryDate"] !== undefined) {
                                resLoan["inquiryDate"] = new Date(inquiryDate[2], inquiryDate[1] - 1, inquiryDate[0])
                            }
                            let amount = resLoan["amount"]
                            if (resLoan["amount"] !== undefined)
                                resLoan["amount"] = parseInt(amount.replace(/,/g, ''));
                        }

                    }

                //}
                //console.log(JSON.stringify(indvRep))
                //client.db("kyros_origination").collection('mock_crif_credit_reports_jan').update({_id:doc._id},{indvReportFile:indvRep})
                client.db("kyros_origination").collection('mock_crif_credit_reports').update({_id:doc._id},{$set: {reportId:doc.reportId,indvReportFile: indvRep}})

            }else{
                client.close();
            };


            console.log("++++++++++++++++++++++++++++++++++++++++++");


        });
    });