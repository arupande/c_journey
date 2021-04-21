var mysql = require('mysql');
var pool;
let kyros_admin_pool;
let kyros_platform_pool;
let util=require('./utility');
var poolExistingCustomer;
let kyros_transaction_pool;
let kyros_comm_pool;
let kyros_payments_pool;
module.exports = {
    getPool: function () {
      let allCloudDetails=util.getAllCloudDetails();
        if (pool) return pool;
        pool = mysql.createPool({
          host     : "10.211.1.171",
          user     : "kyrowrite",
          password : "Kyrowrite@123",
          database : "davinta",
          connectionLimit:50,
          dateStrings:true
        });
        return pool;
    },
  getKyrosAdminPool:function () {
    let kyrosAdminDetails=util.getKyrosAdminDetails();
    if (kyros_admin_pool) return kyros_admin_pool;

    kyros_admin_pool = mysql.createPool({
      host     : "10.211.1.171",
      user     : "kyrowrite",
      password : "davinta",
      database : "Kyrowrite@123",
      connectionLimit:10
    });
    return kyros_admin_pool;
  },
  getKyrosPlatformPool:function () {
    let kyrosPlatformDetails=util.getKyrosPlatformDetails();
    if (kyros_platform_pool) return kyros_platform_pool;
    kyros_platform_pool = mysql.createPool({
      host     : "10.211.1.171",
      user     : "kyrowrite",
      password : "davinta",
      database : "Kyrowrite@123",
      connectionLimit:10
    });
    return kyros_platform_pool;
    },
  getPoolForFindingExistingCustomer: function () {
    let allCloudDetails=util.getAllCloudDetails();

    if (poolExistingCustomer) return poolExistingCustomer;
    poolExistingCustomer = mysql.createPool({
      host     : "10.211.1.171",
      user     : "kyrowrite",
      password : "davinta",
      database : "Kyrowrite@123",
      connectionLimit:50
    });
    return poolExistingCustomer;
  },
  getKyrosTransactionPool:function () {
    let kyrosTrsanctionDetails=util.getKyrosTransactionDetails();
    if (kyros_transaction_pool) return kyros_transaction_pool;
    kyros_transaction_pool = mysql.createPool({
      host     : "10.211.1.171",
      user     : "kyrowrite",
      password : "davinta",
      database : "Kyrowrite@123",
      connectionLimit:10
    });
    return kyros_transaction_pool;
  },
  getKyrosCommPool:function () {
    let db=util.getKyrosCommDetails();
    if (kyros_comm_pool) return kyros_comm_pool;
    kyros_comm_pool = mysql.createPool({
      host     : "10.211.1.171",
      user     : "kyrowrite",
      password : "davinta",
      database : "Kyrowrite@123",
      connectionLimit:100
    });
    return kyros_comm_pool;
  },
  getKyrosPaymentsPool:function() {
    let db=util.getKyrosPaymentsDetails();
    if (kyros_payments_pool) return kyros_payments_pool;
    kyros_payments_pool = mysql.createPool({
      host     : "10.211.1.171",
      user     : "kyrowrite",
      password : "davinta",
      database : "Kyrowrite@123",
      connectionLimit:10
    });
    return kyros_payments_pool;
  }
};
