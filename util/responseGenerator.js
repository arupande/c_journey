let db = require('./mysqlDBConn');
//let _=require('lodash');
let moment = require('moment');
module.exports.errorMessage=(message)=>{
  return {"status":{"statusCode":0,"messageDescription":message,"errorCode":"error.validation"}};
}
module.exports.successMessage=(result,message)=>{
  return {"status":{"statusCode":1,"messageDescription":message?message:"Request completed."},"results":result};
}
module.exports.perpareStringArray=(varList)=>{
  let str = "";
  for( let i = 0; varList &&i < varList.length; i++){
    if(i===0){
      str = "'"+varList[i]+"'"
    }
    else{
      str = str +","+"'"+varList[i]+"'"
    }
  }
  return str
}

module.exports.perpareIntArray=(varList)=>{
  let str = "";
  for( let i = 0; varList &&i < varList.length; i++){
    if(i===0){
      str = str + varList[i]
    }
    else{
      str = str + "," + varList[i]
    }
  }
  return str
}

module.exports.perpareSplitStringArray=(varList,index,delimeter)=>{
  let str = "";
  for( let i = 0; varList &&i < varList.length; i++){
    if(i===0){
      str = "'"+varList[i].split(delimeter)[index]+"'"
    }
    else{
      str = str +","+"'"+varList[i].split(delimeter)[index]+"'"
    }
  }
  return str
}
module.exports.loadQueryFilter=(body={})=>{
  let queryFilter='';
  if (body["FilterState"] && body["FilterState"].length > 0) {
    queryFilter = queryFilter + "\nand upper(csstate.`StateName`) in (" + perpareStringArray(body["FilterState"]) + ")"
  }
  if (body["FilterDistrict"] && body["FilterDistrict"].length > 0) {
    queryFilter = queryFilter + "\nand upper(cs.`StateName`) in (" + perpareStringArray(body["FilterDistrict"]) + ")"
  }
  if (body["FilterProgram"] && body["FilterProgram"].length > 0) {
    queryFilter = queryFilter + "\nand upper(cd.`UDF9`) in (" + perpareStringArray(body["FilterProgram"]) + ")"
  }
  if (body["FilterProduct"] && body["FilterProduct"].length > 0) {
    queryFilter = queryFilter + "\nand upper(s.`SchemeName`) in (" + perpareStringArray(body["FilterProduct"]) + ")"
  }
  if (body["FilterPurpose"] && body["FilterPurpose"].length > 0) {
    queryFilter = queryFilter + "\nand upper(ld.`PurposeofLoan`) in (" + perpareStringArray(body["FilterPurpose"]) + ")"
  }
  if (body["FilterMerchant"] && body["FilterMerchant"].length > 0) {
    queryFilter = queryFilter + "\nand u.`UserId` in (" + perpareIntArray(body["FilterMerchant"]) + ")"
  }
  if (body["FilterTown"] && body["FilterTown"].length > 0) {
    queryFilter = queryFilter + "\nand a.`Town` in (" + perpareSplitStringArray(body["FilterTown"],1,"_") + ")"
  }
  if( body["FilterLoanAmountBucket"] && body["FilterLoanAmountBucket"]["min"] && body["FilterLoanAmountBucket"]["max"] )
    queryFilter = queryFilter + "\nand f.`TotalAmount` between "+ body["FilterLoanAmountBucket"]["min"] + " and " + body["FilterLoanAmountBucket"]["max"]

  if(body["FilterGender"] && body["FilterGender"].length > 0){
    queryFilter = queryFilter + "\nand c.Gender = " + body["FilterGender"]
  }

  if(body["FilterRepaymentFrequency"] && body["FilterRepaymentFrequency"].length > 0){
    if((body["FilterRepaymentFrequency"][0]).toLowerCase().includes('monthly')){
      queryFilter = queryFilter + "\nand lower(s.`SchemeName`) like \'%month%\'\n"
    }else if((body["FilterRepaymentFrequency"][0]).toLowerCase().includes('fort')){
      queryFilter = queryFilter + "\nand lower(s.`SchemeName`) like \'%fortnight%\'\n"
    }
  }

  if(body["FilterTenure"] && body["FilterTenure"].length > 0){
    if(body["FilterTenure"][0].toLowerCase().includes('1')){
      queryFilter=queryFilter+"\nand (f.Duration = 12 and lower(s.`SchemeName`) like '%month%') or (f.Duration = 26 and lower(s.`SchemeName`) like '%fortnight%')"
    }else if(body["FilterTenure"][0].toLowerCase().includes('2'))
      queryFilter = queryFilter + "\nand (f.Duration = 24 and lower(s.`SchemeName`) like '%month%') or (f.Duration = 52 and lower(s.`SchemeName`) like '%fortnight%')"
  }

  let filtLoanAge = body["FilterLoanAge"]||[];
  if(filtLoanAge.length > 0){
    queryFilter = queryFilter +
      "\nand TIMESTAMPDIFF(MONTH, f.StartDate,CURDATE()) >= "+
      filtLoanAge[0].split("-")[0]+
      " and TIMESTAMPDIFF(MONTH, f.StartDate,CURDATE()) < "+
      filtLoanAge[0].split("-")[1]
  }
  return queryFilter;
}

module.exports.updateDate=(query,date,backupDate)=>{
  _.forEach(date,(a)=>{
    let replaceString =`STR_TO_DATE('${a.replaceText}','%Y-%m-%d')`;
    if(a.searchText=="CURDATE()"){
      query=query.replace(/CURDATE\(\)/g,replaceString);
      query=query.replace(/curdate\(\)/g,replaceString);
    }else{
      query=query.replace(new RegExp(a.searchText, 'g'),replaceString);
    }
  });
  return query;
}
module.exports.getBackupDate=()=>{
  return new Promise(((resolve, reject) => {
    statementExecutor("select max(backup_date) as backup_date from report_metadata.AllCloud where backup_data_name = 'AllCloud' and status = 0").then((result)=>{
      let BackupDateCache=result[0].backup_date;
      resolve(BackupDateCache);
    }).catch((ee)=>{
      reject(ee);
    });
    }));
}
module.exports.statementExecutor= statementExecutor=(statement,myPool)=>{//this will be executed in allcloud db;
  return new Promise((resolve, reject) => {
    try {
      let pool;
      if(myPool instanceof Object || myPool instanceof Function){
        pool = myPool;
      }
      else
        pool=db.getPool();

      if (!pool) {
        reject(new Error('Connection is not Available'));
        return;
      }
      pool=db.getPool();
      pool.getConnection(function (err, connection) {
        if (err) {
          reject(err);
          return;
        }
        connection.query(statement, function (err, result, field) {
          if (!err) {
            resolve(result);
          } else {
            reject(err);
          }
          connection.release();
        }).on('error', (err) => {
          console.log('here');
          reject(err);
        });
      });
    }catch (e) {
      console.error(e);
      reject(e);
    }
    });
};

module.exports.statementExecutorComm= (statement)=>{
 return statementExecutor(statement,db.getKyrosCommPool());
}

module.exports.statementExecutorTrsanctionDB=(statement)=>{
  return statementExecutor(statement,db.getKyrosTransactionPool());
}
module.exports.statementExecutorAdminDB=statementExecutorAdminDB=(statement)=>{
  return statementExecutor(statement,db.getKyrosAdminPool());
}

module.exports.statementExecutorPlatformDB = (statement)=>{
  return statementExecutor(statement,db.getKyrosPlatformPool());
}

module.exports.statementExecutorPaymentsDB= statementExecutorPaymentsDB=(statement) =>{
  return statementExecutor(statement,db.getKyrosPaymentsPool());
}


/*module.exports.getDateReplaceArray=(backupdate,customDate)=>{
  let list=[];
  if(backupdate){
    list.push({replaceText:backupdate,searchText:'CURDATE()'});
  }
  if(customDate){
    list.push({replaceText:customDate,searchText:'customDate'})
  }
  return list;
}
module.exports.checkQueryParams = (customerId,merchant_contact,merchantUserId,stateName)=>{
  if(customerId) {
    if (customerId instanceof Array) {
      customerId = customerId.join(',');
    }else if(customerId instanceof String){
      customerId = customerId.split(',').join(',');
    }
  }
  if(merchant_contact) {
    if (merchant_contact instanceof Array) {
      merchant_contact = merchant_contact.join(',');
    }else if(merchant_contact instanceof String){
      merchant_contact = merchant_contact.split(',').join(',');
    }
  }

  if(merchantUserId) {
    if (merchantUserId instanceof Array) {
      merchantUserId = merchantUserId.join(',');
    }else if(merchantUserId instanceof String){
      merchantUserId = merchantUserId.split(',').join(',');
    }
  }
  if(stateName){
    if (stateName instanceof Array) {
      stateName = stateName.join(',');
    }else if(stateName instanceof String){
      stateName = stateName.split(',').join(',');
    }
  }
  return [customerId,merchant_contact,merchantUserId,stateName];
}

module.exports.getMerchantUserIdFromPlatformService=(mobileNumber)=>{
  let statement=`select user_id from kyros_admin.appuser ap where ap.user_type=${3} AND ap.mobile_no=${mobileNumber};`;
  return statementExecutorAdminDB(statement);
};

module.exports.getListOfAllDigitalTransactions =()=>{
  let statement=`select DISTINCT ep.customer_id CustomerId,ep.transfer_type type,ep.narration,ep.payment_id,ep.registration_id,ep.transaction_date,ep.remitter_full_name,ep.loan_id from kyros_payment.ecollect_payment ep where ep.status_cd=702 and ep.validate_response_cd='ACCEPTED' and ep.transaction_amount>10 and ep.transfer_type in ('IMPS','UPI','NEFT','NACH') order by ep.last_modified_date desc;`
  return statementExecutorPaymentsDB(statement)
};


module.exports.getListOfAllMandateRegistration =(customerIds)=>{
  let statement=`select DISTINCT ep.customer_id CustomerId,ep.loan_id, 'NACH' type from kyros_payment.mandate_registration ep where ep.status_cd=802 and ep.customer_id!='' ${customerIds?' and customer_id in ('+customerIds+')':''} order by ep.last_modified_date desc;`
  return statementExecutorPaymentsDB(statement)
};*/


/*module.exports.getListOfAllMandateRegistrationForFinoBank =(customerIds)=>{
  let statement=`select DISTINCT ep.customer_id CustomerId,ep.loan_id, 'FINO' type from kyros_payment.finobank_registration ep where ep.status_cd=604 and ep.customer_id!='' ${customerIds?' and customer_id in ('+customerIds+')':''} order by ep.last_modified_date desc;`
  return statementExecutorPaymentsDB(statement)
};

module.exports.buildTransactionMapping=(list)=>{
  let json={};
  list && list.length && list.map(v=>json[v.CustomerId]=v);
  return json;
}*/
/*const getRandomNumberInRange=(min,max)=>{
  return Math.floor(Math.random() * (max - min) + min);
}*/
/*module.exports.getDataSetWithRandomSelection= (list,totalDivide)=>{
  let selectionMap={};
  for(let i=0;i<list.length;i++){
    selectionMap[i]=false;
  }
  let eachListCount= Math.floor(list.length/totalRandom);
  let totalRandom=0;
  while(totalRandom==list.length){
  }
};*/

/*module.exports.getRandomListClosure=function(list){
  let selectionMatrix={};
  list.forEach((v,i)=>selectionMatrix[i+1]=false);
  function getRandomListLogic(count){
    let randomList=[];
    let totalFill=count;
    let totalKeys=Object.values(selectionMatrix);
    if(!totalKeys.includes(false)){
      return [];
    }
    let maxCountExecution=list.length*100;//maximum while loop execution limit;
    while(totalFill!==0 && randomList.length!==count && maxCountExecution!=0){
      let r = Math.floor((Math.random()*(list.length))+1);
      if(!selectionMatrix[r]){
        selectionMatrix[r]=true;
        randomList.push(list[r-1]);
        totalFill--;
      }
      maxCountExecution--;
    }
    return randomList;
  }
  return getRandomListLogic;
}*/

/*let FinanceIdMappingWithLastEmiDateCache,LastUpdatedDate_cache;
module.exports.getFinanceIdMappingWithLastEmiDate=async ()=>{
  if(FinanceIdMappingWithLastEmiDateCache &&  LastUpdatedDate_cache && moment.utc().diff(LastUpdatedDate_cache,'hours')<=24){
    console.log('backupdate serve from cache');
    return FinanceIdMappingWithLastEmiDateCache;
  }
  let statement='select f.AgreementNo agg,f.FinanceId, max(p.duedate) LastEmi from payments p left join finances f on f.FinanceId=p.FinanceId group by p.FinanceId;'
  let result = await statementExecutor(statement);
  let finalResult={};
  result.forEach(v=>{
    finalResult[v["agg"]]=v["LastEmi"];
  });
  FinanceIdMappingWithLastEmiDateCache=finalResult;
  LastUpdatedDate_cache=moment.utc();
  return finalResult;
};*/


//let pincodeList = require('./../config/Partner_Support_Active_Pincode_MSME.json');
/* GET home page. */

/*let totalPinCodeList=[];
module.exports.checkActivePinCodeForMsme=(pincode)=>{
  if(totalPinCodeList.length==0){
    pincodeList.forEach((v)=>{
      totalPinCodeList=[...totalPinCodeList,...v.pincode];
    });
  }
  let isAnyMatchFound=false;
  pincode=pincode.split('');
  for(let i=0;i<totalPinCodeList.length;i++){
    if(totalPinCodeList[i]===pincode[0]+pincode[1]){
      isAnyMatchFound=true;
      break;
    }
  }
  return isAnyMatchFound;
};*/
/*module.exports.validateMobileNo= (mobileNo)=>{
    return mobileNo && mobileNo.length==10 && mobileNo.match(/^[0-9]*$/);
};

const columnMapping={
  "customerId":0,
  "firstName":3,
  "loan_status":1,
  "tenure":7
};*/
/*module.exports.transformTopUpExcel=transformTopUpExcel=(data)=>{
  let transformedData={};
  if(data.length){
    for(let i=1;i<data.length;i++){
      let t=data[i];
      transformedData[t[columnMapping.customerId]]={
        isEligible:true,
        tenure:t[columnMapping.tenure],
        originalData:t
      }
    }
  }
  return transformedData;
};*/

/*
module.exports.isEligibleForTopUp=(id)=>{
if(EXCELDATA && Object.keys(EXCELDATA).length){
  return EXCELDATA[id];
}else{
  throw "Topup.xlsx file is missing or unable to parse;"
}
};
let XLSX= require('xlsx');
let EXCELDATA =null;
setTimeout(function(){EXCELDATA=transformTopUpExcel(load_data(__dirname+'/../config/Topup.xlsx'));},10);
*/

/*function load_data(file) {
  try {
    let wb = XLSX.readFile(file);
    /!* generate array of arrays *!/
    let data = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]], {header: 1});
    return data;
  }
  catch(e){
    console.error(e);
    throw "Unable to locate file or file is not readable";
  }
};*/
