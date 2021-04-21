const {ObjectID} = require('mongodb');
var MongoClient = require('mongodb').MongoClient;
var moment = require('moment');
let LastUpdatedDate='';
MongoClient.connect('mongodb://localhost:27017/kyros_origination',
    function(err, client) {

        if(err) throw(err);

        var cur = client.db("kyros_origination").collection('crif_credit_reports').find({/*'_id':ObjectID('5e28078257e8401082f0a984')*/});
        var updates = 0;
        var curDone=false;
        LastUpdatedDate = moment.utc();
        cur.each(function(err, doc){
            if(err) throw err;
            if (doc != null){
                let indvRep = doc.indvReportFiles;
                for (var i = 0; i < indvRep.length; i++) {
                    let indvReportFiles = doc.indvReportFiles[i];
                    var res1 = indvReportFiles.indvReportFile.indvReports.indvReport.responses;
                    if (typeof res1 == 'object' && res1) {
                        let res = res1["response"];
                        for (var j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            var maxLoanList = [];
                            var noOfDPD = 0;
                            var combinedPaymentHistoryList = resLoan["loanDetails"]["combinedPaymentHistory"].split("|");
                            combinedPaymentHistoryList.forEach(combinedPaymentHistory => {
                                if (combinedPaymentHistory !== "undefined" && combinedPaymentHistory !== "") {
                                    let dpd = parseInt(combinedPaymentHistory.split(",")[1].split("/")[0]
                                        .replace("DDD", "0")
                                        .replace("XXX", "0")
                                        .replace("DDD", "0")
                                    );
                                    if (dpd > 0) {
                                        noOfDPD++
                                    }
                                    maxLoanList.push(dpd);
                                    resLoan["loanDetails"]["maxLoanList"] = maxLoanList;
                                    resLoan["loanDetails"]["maxDPD"] = Math.max.apply(Math, maxLoanList);
                                    resLoan["loanDetails"]["noEMI"] = maxLoanList.length;
                                    resLoan["loanDetails"]["noOfDPD"] = noOfDPD;
                                }
                            });
                            let disbursedAmt = resLoan["loanDetails"]["disbursedAmt"];
                            if (resLoan["loanDetails"]["disbursedAmt"] !== undefined)
                                resLoan["loanDetails"]["disbursedAmt"] = parseInt(disbursedAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetails"] && resLoan["loanDetails"]["disbursedAmt"] == undefined)
                                resLoan["loanDetails"]["disbursedAmt"] = 0;
                            let overdueAmt = resLoan["loanDetails"]["overdueAmt"];
                            if (resLoan["loanDetails"]["overdueAmt"] !== undefined)
                                resLoan["loanDetails"]["overdueAmt"] = parseInt(overdueAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetails"] && resLoan["loanDetails"]["overdueAmt"] == undefined)// New Code Change
                                resLoan["loanDetails"]["overdueAmt"] = 0;
                            let writeOffAmt = resLoan["loanDetails"]["writeOffAmt"];
                            if (resLoan["loanDetails"]["writeOffAmt"] !== undefined)
                                resLoan["loanDetails"]["writeOffAmt"] = parseInt(writeOffAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetails"] && resLoan["loanDetails"]["writeOffAmt"] == undefined) //New Code Change
                                resLoan["loanDetails"]["writeOffAmt"] = 0;
                            let currentBal = resLoan["loanDetails"]["currentBal"];
                            if (resLoan["loanDetails"]["currentBal"] !== undefined)
                                resLoan["loanDetails"]["currentBal"] = parseInt(currentBal.toString().replace(/,/g,''));
                            if (resLoan["loanDetails"] && resLoan["loanDetails"]["currentBal"] == undefined) //New Code Change
                                resLoan["loanDetails"]["currentBal"] = 0;
                        }
                    }
                    let indv = indvReportFiles.indvReportFile.indvReports.indvReport.indvResponses.indvResponseList;
                    if (typeof indv == 'object' && indv) {
                        let res = indv["indvResponse"];
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let maxLoanList = [];
                            let noOfDPD = 0;
                            let combinedPaymentHistoryList = resLoan["loanDetail"]["combinedPaymentHistory"].split("|");
                            combinedPaymentHistoryList.forEach(combinedPaymentHistory => {
                                if (combinedPaymentHistory !== undefined && combinedPaymentHistory !== "") {
                                    let dpd = parseInt(combinedPaymentHistory.split(",")[1]
                                        .replace("DDD", "0")
                                        .replace("XXX", "0")
                                        .replace("DDD", "0")
                                    );

                                    if (dpd !== undefined && dpd > 0) {
                                        noOfDPD++;
                                    }
                                    if (maxLoanList != undefined) {
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
                    if (typeof indv == 'object' && indv) {
                        let res = indv["indvResponse"];
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let disbursedAmt = resLoan["loanDetail"]["disbursedAmt"];
                            if (resLoan["loanDetail"]["disbursedAmt"] !== undefined)
                                resLoan["loanDetail"]["disbursedAmt"] = parseInt(disbursedAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["disbursedAmt"] == undefined)
                                resLoan["loanDetail"]["disbursedAmt"] = 0
                            let overdueAmt = resLoan["loanDetail"]["overdueAmt"];
                            if (resLoan["loanDetail"]["overdueAmt"] !== undefined)
                                resLoan["loanDetail"]["overdueAmt"] = parseInt(overdueAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["overdueAmt"] == undefined)
                                resLoan["loanDetail"]["overdueAmt"] = 0
                            let writeOffAmt = resLoan["loanDetail"]["writeOffAmt"];
                            if (resLoan["loanDetail"]["writeOffAmt"] !== undefined)
                                resLoan["loanDetail"]["writeOffAmt"] = parseInt(writeOffAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["writeOffAmt"] == undefined)
                                resLoan["loanDetail"]["writeOffAmt"] = 0
                            let currentBal = resLoan["loanDetail"]["currentBal"];
                            if (resLoan["loanDetail"]["currentBal"] !== undefined)
                                resLoan["loanDetail"]["currentBal"] = parseInt(currentBal.toString().replace(/,/g,''))
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["currentBal"] == undefined)
                                resLoan["loanDetail"]["currentBal"] = 0
                        }
                    }
                    let grp = indvReportFiles.indvReportFile.indvReports.indvReport.grpResponses.grpResponseList;
                    if (typeof grp == 'object' && grp) {
                        let res = grp["grpResponse"];
                        for(let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let maxLoanList = [];
                            let noOfDPD = 0;
                            let combinedPaymentHistoryList = resLoan["loanDetail"]["combinedPaymentHistory"].split("|");
                            combinedPaymentHistoryList.forEach(combinedPaymentHistory => {
                                if (combinedPaymentHistory !== undefined && combinedPaymentHistory !== "") {
                                    let dpd = parseInt(combinedPaymentHistory.split(",")[1]
                                        .replace("DDD", "0")
                                        .replace("XXX", "0")
                                        .replace("DDD", "0")
                                    );
                                    if (dpd !== undefined && dpd > 0) {
                                        noOfDPD++;
                                    }
                                    if (maxLoanList != undefined) {
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
                    if (typeof grp == 'object' && grp) {
                        let res = grp["grpResponse"];
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let disbursedAmt = resLoan["loanDetail"]["disbursedAmt"];
                            if (resLoan["loanDetail"]["disbursedAmt"] !== undefined)
                                resLoan["loanDetail"]["disbursedAmt"] = parseInt(disbursedAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["disbursedAmt"] == undefined)
                                resLoan["loanDetail"]["disbursedAmt"] = 0
                            let overdueAmt = resLoan["loanDetail"]["overdueAmt"];
                            if (resLoan["loanDetail"]["overdueAmt"] !== undefined)
                                resLoan["loanDetail"]["overdueAmt"] = parseInt(overdueAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["overdueAmt"] == undefined)
                                resLoan["loanDetail"]["overdueAmt"] = 0
                            let writeOffAmt = resLoan["loanDetail"]["writeOffAmt"];
                            if (resLoan["loanDetail"]["writeOffAmt"] !== undefined)
                                resLoan["loanDetail"]["writeOffAmt"] = parseInt(writeOffAmt.toString().replace(/,/g,''));
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["writeOffAmt"] == undefined)
                                resLoan["loanDetail"]["writeOffAmt"] = 0
                            let currentBal = resLoan["loanDetail"]["currentBal"];
                            if (resLoan["loanDetail"]["currentBal"] !== undefined)
                                resLoan["loanDetail"]["currentBal"] = parseInt(currentBal.toString().replace(/,/g,''))
                            if (resLoan["loanDetail"] && resLoan["loanDetail"]["currentBal"] == undefined)
                                resLoan["loanDetail"]["currentBal"] = 0
                        }
                    }
                    let inqHist = indvReportFiles.indvReportFile.indvReports.indvReport.inquiryHistory;
                    if (typeof inqHist == 'object' && inqHist) {
                        let res = inqHist["history"];
                        for (let j = 0; j < res.length; j++) {
                            let resLoan = res[j];
                            let inquiryDate = resLoan["inquiryDate"].toString().split("-");
                            if (resLoan["inquiryDate"] !== undefined && inquiryDate.length==3) {
                                resLoan["inquiryDate"] = new Date(inquiryDate[2], inquiryDate[1] - 1, inquiryDate[0])
                            }
                            var amount = resLoan["amount"];
                            if (resLoan["amount"] !== undefined)
                                resLoan["amount"] = parseInt(amount.toString().replace(/,/g,''));
                        }
                    }
                }
                //console.log(JSON.stringify(indvRep))
                client.db("kyros_origination").collection('crif_credit_reports').update({_id:doc._id},{$set: {reportId:doc.reportId,indvReportFiles: indvRep}})


            }else{
                client.close();
            }


            console.log("++++++++++++++++++++++++++++++++++++++++++");


        });
    });