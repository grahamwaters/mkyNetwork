/****************************
BitMonky Web Moniter Tool
****************************
*/

// This code defines a JavaScript class called MkyDbMonitor that appears to be used to monitor a database and send reports to a user. The MkyDbMonitor class takes two arguments in its constructor, db and bank, which are likely references to the MySQL database and some other object, respectively.

// The MkyDbMonitor class has several methods, including handleReq(), sendReport(), sendGoldTranLog(), sendBlocks(), sendWallets(), getLastBlockNumber(), getDbTime(), and getBChainInfo().

// The handleReq() method is called with two arguments, j and res, and appears to check the value of the req and what properties of the j object. Based on the values of these properties, the method calls one of the other methods of the MkyDbMonitor class and returns the result to the user by calling the end() method on the res object.
class MkyDbMonitor {
  constructor(db,bank){
    this.db = db;
    this.bank = bank;
  }

  async handleReq(j,res){
    if (j.req){
      if (j.what == 'report'){
        var report = await this.sendReport();
        res.end(report);
        return true;
      }
      if (j.what == 'gTranLog'){
        var report = await this.sendGoldTranLog();
        res.end(report);
        return true;
      }
      if (j.what == 'bcBlocks'){
        var report = await this.sendBlocks();
        res.end(report);
        return true;
      }
      if (j.what == 'gWallets'){
        var report = await this.sendWallets();
        res.end(report);
        return true;
      }
    }
    return false
  }
  sendReport(){
    return new Promise( async (resolve,reject)=>{
      var lastBlock = await this.getLastBlockNumber();
      var time      = await this.getDbTime();
      var SQL =  "select count(*)nRec from tblGoldTrans "; //where not gtrnSource='BMiner Reward' ";
      this.db.query(SQL , (err, result,fields)=>{
        if (err){console.log(err); resolve(err); }
        else {
          var nRec = result[0].nRec;

          SQL =  "select * from tblGoldTrans ";
          SQL += "order by gtrnDate desc,gtrnSyncKey desc limit 15 ";
          this.db.query(SQL , (err, result,fields)=>{
            if (err){console.log(err); resolve(err); }
            else {
              var trans = [];
              const dbres = Object.keys(result);
              dbres.forEach(function(key) {
                var tRec = result[key];
                trans.push(tRec);
              });
              var bstat = 'Not Ready';
              if (this.bank){
                bstat = this.bank.status;
              }
              var response = {
                trans : trans,
                lastBlock : lastBlock,
                dbTime : time,
                bstat  : bstat,
                nRec   : nRec,
                maxBlockSize : this.bank.maxBlockSize,
                logsRotating  : this.bank.logsRotating
              }
              resolve(JSON.stringify(response));
            }
          });
        }
      });
    });
  }
  sendGoldTranLog(){
    return new Promise( async (resolve,reject)=>{
      var lastBlock = await this.getLastBlockNumber();
      var time      = await this.getDbTime();
      var SQL =  "select * from tblGoldTranLog ";
      SQL += "order by gtlDate desc,gTranLogID desc limit 15 ";
      this.db.query(SQL , function (err, result,fields) {
        if (err){console.log(err); resolve(err); }
        else {
          var trans = [];
          const dbres = Object.keys(result);
          dbres.forEach(function(key) {
            var tRec = result[key];
            trans.push(tRec);
          });
          var response = {
            trans : trans,
            lastBlock : lastBlock,
            dbTime    : time
          }
          resolve(JSON.stringify(response));
        }
      });
    });
  }
  sendBlocks(){
    return new Promise( async (resolve,reject)=>{
      var time      = await this.getDbTime();
      var hRate     = await this.bank.chain.getChainHashRate('tblGoldTranLog');
      var bcha      = await this.getBChainInfo(1);
      var SQL =  "select * from mkyBlockC.tblmkyBlocks ";
      SQL += "order by blockNbr desc limit 15 ";
      this.db.query(SQL , (err, result,fields)=>{
        if (err){console.log(err); resolve(err); }
        else {
          var trans = [];
          const dbres = Object.keys(result);
          dbres.forEach(function(key) {
            var tRec = result[key];
            trans.push(tRec);
          });
          var response = {
            trans : trans,
            dbTime    : time,
            hRate     : hRate,
            diff      : bcha.bchaDifficulty,
            tick      : this.bank.chain.hrTicker
          }
          resolve(JSON.stringify(response));
        }
      });
    });
  }
  sendWallets(){
    return new Promise( async (resolve,reject)=>{
      var time      = await this.getDbTime();
      var SQL =  "select * from mkyBank.tblmkyWallets ";
      SQL += "order by mwalMUID desc limit 15 ";
      this.db.query(SQL , function (err, result,fields) {
        if (err){console.log(err); resolve(err); }
        else {
          var trans = [];
          const dbres = Object.keys(result);
          dbres.forEach(function(key) {
            var tRec = result[key];
            trans.push(tRec);
          });
          var response = {
            trans : trans,
            dbTime    : time
          }
          resolve(JSON.stringify(response));
        }
      });
    });
  }
  getBChainInfo(chainID){
     return new Promise( (resolve,reject)=>{
       var SQL  = "SELECT * from mkyBlockC.tblmkyBlockChain where bchaID = "+chainID;
       this.db.query(SQL, async function (err, result, fields) {
         if (err) {console.log(err); resolve(null);}
         else {
           if (result.length  > 0) {
             const rec = result[0];
             resolve(rec);
           }
           else {
             resolve(null);
           }
        }
      });
    });
  }
  getDbTime(){
     return new Promise( (resolve,reject)=>{
       var SQL  = "SELECT now() as tnow"
       this.db.query(SQL, async function (err, result, fields) {
         if (err) {console.log(err); resolve(err);}
         else {
           if (result.length  > 0) {
             const rec = result[0];
             resolve(rec.tnow);
           }
           else {
             resolve(0);
           }
        }
      });
    });
  }
  getLastBlockNumber(){
    return new Promise( (resolve,reject)=>{
      var SQL  = "SELECT gtrnBlockID  FROM mkyBank.tblGoldTrans ";
      SQL += "group by gtrnBlockID ";
      SQL += "union ";
      SQL += "SELECT gtlBlockID  FROM mkyBank.tblGoldTranLog ";
      SQL += "group by gtlBlockID ";
      SQL += "order by gtrnBlockID desc limit 1 ";
      //console.log( "\n"+SQL);
      this.db.query(SQL, async function (err, result, fields) {
        if (err) {console.log(err); resolve(err);}
        else {
          if (result.length  > 0) {
            const rec = result[0];
            const lastBLID = rec.gtrnBlockID;
            resolve(lastBLID);
          }
          else {
            resolve(0);
          }
        }
      });
    });
  }
};
module.exports.MkyDbMonitor = MkyDbMonitor;
