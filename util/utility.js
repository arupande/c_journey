let allcloud,
    kyros_admin,
    kyros_platform,
    kyros_origination,
    kyros_transaction,
    kyros_comm,
    kyros_payment
;
module.exports.init=()=>{
  this.getAllCloudDetails(true);
  this.getKyrosAdminDetails(true);
  this.getKyrosPlatformDetails(true);
  this.getKyrosOriginationDetails(true);
};
module.exports.getAllCloudDetails= (isRefresh)=>{
  if(allcloud && !isRefresh)
    return allcloud;

  return allcloud={
    host:process.env.allcloud_host,
    user:process.env.allcloud_user,
    password:process.env.allcloud_pass,
    database:process.env.allcloud_database
  }
};

module.exports.getKyrosAdminDetails=(isRefresh)=>{
  if(kyros_admin && !isRefresh)
    return kyros_admin;

  return kyros_admin={
    host:process.env.kyros_admin_host,
    user:process.env.kyros_admin_user,
    password:process.env.kyros_admin_pass,
    database:process.env.kyros_admin_database
  }
};

module.exports.getKyrosPlatformDetails =(isRefresh)=>{
  if (kyros_platform && !isRefresh) return kyros_platform;
  return kyros_platform = {
    host     : process.env.kyros_platform_host,
    user     : process.env.kyros_platform_user,
    password : process.env.kyros_platform_pass,
    database :process.env.kyros_platform_database,
  };
};
module.exports.getKyrosTransactionDetails =(isRefresh)=>{
  if (kyros_transaction && !isRefresh) return kyros_transaction;
  return kyros_transaction = {
    host     : process.env.kyros_transaction_host,
    user     : process.env.kyros_transaction_user,
    password : process.env.kyros_transaction_pass,
    database :process.env.kyros_transaction_database,
  };
};

module.exports.getKyrosPaymentsDetails = (isRefresh)=>{
  if (kyros_payment && !isRefresh) return kyros_payment;
  return kyros_payment = {
    host     : process.env.kyros_payment_host,
    user     : process.env.kyros_payment_user,
    password : process.env.kyros_payment_pass,
    database :process.env.kyros_payment_database,
  };
};


module.exports.getKyrosOriginationDetails = (isRefresh)=> {
  if (kyros_origination && !isRefresh) return kyros_origination;
  return kyros_origination = {
    host     : process.env.kyros_origination_host,
    user     : process.env.kyros_origination_user,
    password : process.env.kyros_origination_pass,
    database :process.env.kyros_origination_database,
  };
};

module.exports.getKyrosCommDetails = (isRefresh)=> {
  if (kyros_comm && !isRefresh) return kyros_comm;
  return kyros_comm = {
    host     : process.env.kyros_comm_host,
    user     : process.env.kyros_comm_user,
    password : process.env.kyros_comm_pass,
    database :process.env.kyros_comm_database,
  };
};
module.exports.getEnvironment=()=>{
  return process.env.helios_environment||"local";
};
module.exports.isDevelopment =()=>{
  return ["local","develop","development"].includes(process.env.helios_environment||"local");
}
module.exports.validateConfiguration=()=>{

};
module.exports.originationServiceHost=()=>{
  return process.env.origination_service_host;
};
module.exports.transactionServiceHost=()=>{
  return process.env.transaction_service_host;
};
module.exports.adminServiceHost=()=>{
  return process.env.admin_service_host;
};
module.exports.getIvrVendorToken=()=>{
  return process.env.ivr_vendor_token;
};
module.exports.getIvrVendorKey=()=>{
  return process.env.ivr_vendor_key;
};
module.exports.getIvrVendorAccount=()=>{
  return process.env.ivr_vendor_account;
};
module.exports.getKyrosPaymentService=()=>{
  return process.env.payment_service_host;
};
