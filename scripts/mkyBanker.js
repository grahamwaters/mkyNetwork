//const config       = require('./config.js');
var dateFormat     = require('dateformat'); // this is the date format library
const EventEmitter = require('events'); // this is the event emitter library
const https        = require('https'); // this is the https library
const fs           = require('fs'); // this is the file system library
const mkyPubKey    = '04a5dc8478989c0122c3eb6750c08039a91abf175c458ff5d64dbf448df8f1ba6ac4a6839e5cb0c9c711b15e85dae98f04697e4126186c4eab425064a97910dedc';
const EC           = require('elliptic').ec; // this is the elliptic curve library
const ec           = new EC('secp256k1'); // this is the elliptic curve we are using
const crypto       = require('crypto'); // this is the crypto library
const mysql        = require('mysql'); // this is the mysql library
const schedule     = require('node-schedule'); // this is the node-schedule library
const {MkyBlock}   = require('./mkyBlock'); // this is the mkyBlock class
const {MkyWallet}  = require('./mkyWallet'); // this is the mkyWallet class
const {BranchList} = require('./mkyBranchList.js'); // this is the BranchList class
const {MkyBlockChainMgr,RegTransaction} = require('./mkyBlockChainMgr.js'); // this is the BlockChainMgr class
const {MkyBankGroupMgr,MkyPeer}         = require('./mkyBankGroup.js'); // this is the BankGroupMgr class
const {MkyWebConsole}                   = require('./networkWebConsole.js');  // this is the networkWebConsole class

addslashes  = require ('./addslashes'); // this is the addslashes function

var con = mysql.createConnection({ // this is the mysql connection
  host: "localhost",
  user: "username",
  password: "password",
  database: "mkyBank",
  dateStrings: "date",
  multipleStatements: true,
  supportBigNumbers : true
});
con.connect(function(err) {
  if (err) throw err; // if there is an error throw it
});
var bcTypes = [];
bcTypes.push('tblGoldTrans'); // this is the list of tables that are part of the blockchain
bcTypes.push('tblGoldTranLog'); // this is the list of tables that are part of the blockchain

function getRandomInt(max) {
  // """ this function returns a random integer between 0 and max """
  return Math.floor(Math.random() * Math.floor(max));
}

// this is the main class for the mkyBanker
class Reg2Transaction{
  // The Reg2Transaction class appears to be a class that represents a transaction in a cryptocurrency system. It has a number of methods that are used to process and save transactions to a database.

  // The constructor method is used to create an instance of the Reg2Transaction class and takes three parameters: tran, db, and sig. tran is the transaction data, db is a database connection, and sig is a signature.

  // The calculateHash method calculates the hash of the transaction data and returns it. The saveTransaction method saves the transaction data to the database, if it is not already there. The getWalletPubKey method retrieves the public key for a wallet from the database using the wallet's MUID. The confirmAndSave method confirms the transaction and saves it to the database if it is valid. The isValid method checks if the transaction is valid by verifying the signature and checking if the transaction is already in the database. The verifySignature method verifies the signature of the transaction.
  constructor(tran,db,sig,bctr){
    this.tran = tran;
    this.db   = db;
    this.sig  = sig;
    this.bctr = bctr;
  }
  calculateHash() {
    return crypto.createHash('sha256').update(JSON.stringify(this.tran)).digest('hex');
  }
  saveTransaction(){
    /* increment block counter and save */
    const gtrnBlockID = 'null'; //this.bctr.nbr;
    this.bctr.nRec++; // increment the number of records in the block
    if (this.bctr.nRec >= this.bctr.maxBlockSize){
      this.bctr.nRec = 0; // reset the number of records in the block
      this.bctr.nbr++;  // increment the block number
    }
    const tran = this.tran; // this is the transaction
    var SQL = "Select count(*)nRec from tblGoldTrans where gtrnSyncKey = '"+tran.syncKey+"'";
    var db = this.db // this is the database connection
    this.db.query(SQL, function (err, result, fields) {
      if (err) console.log(err); // if there is an error, log it
      else {
        if (result[0].nRec == 0){ // if the transaction is not already in the database
          SQL = "insert into tblGoldTrans (gtrnAmount,gtrnGoldType,gtrnSource,gtrnSrcID,gtrnTycTax,gtrnTaxHold,gtrnCityID,gtrnGoldRate,gtrnMUID,";
          SQL += "gtrnSyncKey,gtrnSignature,gtrnDate,gtrnQAppm,gtrnBlockID) ";
          SQL += "values ("+tran.gtlAmount+",'"+tran.gtlGoldType+"','"+tran.gtlSource+"',"+tran.gtlSrcID+","; // add the transaction data
          SQL += tran.gtlTycTax+","+tran.gtlTaxHold+","+tran.gtlCityID+","+tran.gtlGoldRate+",'"+tran.gtlMUID+"',"; // add the transaction data
          SQL += "'"+tran.syncKey+"','"+this.sig+"','"+tran.gtlDate+"','"+tran.gtlQApp+"',"+gtrnBlockID+")"; // save the transaction
         //console.log('Reg2Trans confirm and save ',tran.gtlGoldType);
          db.query(SQL, function (err, result, fields) { // save the transaction
            if (err) console.log(err); // if there is an error, log it
          });
        }
      }
    });
  }
  getWalletPubKey(){
    // """ this function returns the public key for the wallet """
    const tran = this.tran; // this is the transaction
    return new Promise( (resolve,reject)=>{
      const SQL = "select mwalPubKey from tblmkyWallets where mwalMUID = '"+tran.gtlMUID+"'";
      this.db.query(SQL, function (err, result, fields) { // get the wallet public key
        if (err) {console.log(err);resolve(null);}  // if there is an error, log it
        else {  // if there is no error
          if (result.length == 0){  // if there is no wallet for this MUID
            resolve(mkyPubKey); // return the default public key
          }
          else
            resolve(result[0].mwalPubKey);  // return the wallet public key
        }
      });

    });
  }
  async confirmAndSave() {
    // """ this function confirms the transaction and saves it """
    if (!this.tran.gtlMUID){ // if there is no MUID in the transaction
     //console.log('No Wallet MUID in this transaction');
      return;
    }
    if (!this.sig || this.sig.length === 0) { // if there is no signature in the transaction
     //console.log ('No signature in this transaction');
      return;
    }
    const wKey = await this.getWalletPubKey();
    if (!wKey){ // if there is no public key for the wallet
     //console.log ('Empty Wallet Public Key Found.');
      return;
    }
    const publicKey = ec.keyFromPublic(wKey, 'hex'); // get the public key
    if (publicKey.verify(this.calculateHash(), this.sig)){ // if the signature matches the transaction
      //this.saveTransaction();
     //console.log('Tran Reverification good');
    }
    else { // if the signature does not match the transaction
     //console.log('Banker: Trans verification Fail... transaction dropped.');
      //console.log('signature tried',this.sig);
      //console.log('wpubkey',wKey);
    }
  }
}
class MkyTransaction {
  constructor(fromWallet,tran,signature,tdate,bctr) {
      if (tran.syncKey == 'NA')
        tran.syncKey = crypto.createHash('sha256').update(JSON.stringify(tran)).digest('hex');

      this.tran          = tran; // this is the transaction
      this.fromWallet    = fromWallet; // this is the wallet that sent the transaction
      this.gtrnGoldType  = tran.gtlGoldType; // this is the gold type
      this.gtrnSource    = tran.gtlSource; // this is the source of the transaction
      this.gtrnSrcID     = tran.gtlSrcID; // this is the source ID
      this.gtrnTycTax    = tran.gtlTycTax; // this is the tax code
      this.gtrnAmount    = tran.gtlAmount;  // this is the amount of gold
      this.gtrnCityID    = tran.gtlCityID;  // this is the city ID
      this.gtrnTaxHold   = tran.gtlTaxHold; // this is the tax hold
      this.gtrnGoldRate  = tran.gtlGoldRate;  // this is the gold rate
      this.gtrnSyncKey   = tran.syncKey;  // this is the sync key
      this.gtrnQApp      = tran.gtlQApp;  // this is the QApp
      this.gtrnMUID      = tran.gtlMUID;  // this is the MUID
      this.gtrnSignature = signature; // this is the signature of the transaction
      this.gtrnDate      = tdate; // this is the date of the transaction
      this.bctr          = bctr;  // this is the block counter
  }

  calculateHash() {
    // """ this function calculates the hash of the transaction """
    return crypto.createHash('sha256').update(JSON.stringify(this.tran)).digest('hex');
  }
  signTransaction(signingKey) {
    // """ this function signs the transaction """
    if (signingKey.getPublic('hex') !== this.fromWallet) {
      throw new Error('You cannot sign transactions for other wallets!');
    }
    // Calculate the hash of this transaction, sign it with the key
    // and store it inside the transaction obect
    const hashTx = this.calculateHash();
    const sig = signingKey.sign(hashTx, 'base64');
    //console.log('trans signature: ',sig);
    this.signature = sig.toDER('hex');
   //console.log('trans this.signature: '+ this.signature.length + ': ',this.signature);
  }
  testTran(sKey){
    // """ this function tests the transaction """
    var SQL = "select gtrnDate,gtrnGoldType,gtrnSource,cast(gtrnSrcID as char)gtrnSrcID,cast(gtrnTycTax as char)gtrnTycTax,";
    SQL += "cast(gtrnAmount as char)gtrnAmount,cast(gtrnCityID as char)gtrnCityID,";
    SQL += "cast(gtrnTaxHold as char)gtrnTaxHold,cast(gtrnGoldRate as char)gtrnGoldRate,gtrnSyncKey,gtrnQApp,gtrnMUID,gtrnSignature ";
    SQL += " from tblGoldTrans where gtrnSyncKey = '"+sKey+"'";
    con.query(SQL , function (err, result,fields) {
      if (err){
       //console.log('"blockErr":' + JSON.stringify(err)+'}');
      }
      else { // if there are no errors
        var trans = []; // this is the array of transactions
        const dbres = Object.keys(result); // this is the array of keys
        dbres.forEach(function(key) { // for each key
          var tRec = result[key]; // this is the transaction record
          trans.push(tRec); // add the transaction to the array
        });
        if (trans.length == 0)
          console.log('{"result":"No Transactions To Send Right Now."}',SQL);
        else {
          var myResponse = {
            bLastTrans : trans
          }
          for (var tran of trans){
            var trand = {
              gtlDate     : tran.gtrnDate,
              gtlGoldType : tran.gtrnGoldType,
              gtlSource   : tran.gtrnSource,
              gtlSrcID    : tran.gtrnSrcID,
              gtlTycTax   : tran.gtrnTycTax,
              gtlAmount   : tran.gtrnAmount,
              gtlCityID   : tran.gtrnCityID,
              gtlTaxHold  : tran.gtrnTaxHold,
              gtlGoldRate : tran.gtrnGoldRate,
              syncKey     : tran.gtrnSyncKey,
              gtlQApp     : tran.gtrnQApp,
              gtlMUID     : tran.gtrnMUID
            }
            var sig = tran.gtrnSignature;

            var conf = new Reg2Transaction(trand,con,tran.gtrnSignature,this.bctr);
            conf.confirmAndSave();
            this.lastTran =  tran.gtrnDate;
          }
        }
      }
    });
  }
  saveTransaction(){
    /* increment block counter and save */
    this.gtrnBlockID = 'null'; //this.bctr.nbr;
/*
    this.bctr.nRec++;
    if (this.bctr.nRec >= this.bctr.maxBlockSize){
      this.bctr.nRec = 0;
      this.bctr.nbr++;
    }
*/
    const tran = this;
    var SQL = "insert into tblGoldTrans (gtrnAmount,gtrnGoldType,gtrnSource,gtrnSrcID,gtrnTycTax,gtrnTaxHold,gtrnCityID,gtrnGoldRate,gtrnMUID,";
    SQL += "gtrnSyncKey,gtrnSignature,gtrnDate,gtrnQApp,gtrnBlockID) ";
    SQL += "values ("+this.gtrnAmount+",'"+this.gtrnGoldType+"','"+this.gtrnSource+"',"+this.gtrnSrcID+",";
    SQL += this.gtrnTycTax+","+this.gtrnTaxHold+","+this.gtrnCityID+","+this.gtrnGoldRate+",'"+this.gtrnMUID+"',";
    SQL += "'"+this.gtrnSyncKey+"','"+this.gtrnSignature+"','"+this.gtrnDate+"','"+this.gtrnQApp+"',"+this.gtrnBlockID+")";
    con.query(SQL, function (err, result, fields) {
      if (err)//console.log(err);
      tran.testTran(tran.gtrnSyncKey);
    });
  }
  isValid() {
   //console.log('checking BitMonky Transaction For Signatures');
    if (this.fromWallet === null) return false;

    if (!this.gtrnSignature || this.gtrnSignature.length === 0) {
      throw new Error('No signature in this transaction');
    }

    const publicKey = ec.keyFromPublic(this.fromWallet, 'hex');
    return publicKey.verify(this.calculateHash(), this.gtrnSignature);
  }
}
class MkyBank {
  // """ This is the Bank Class """
  constructor(branchId,branchIp,branchNetwork,reset,resetTo=null){
    this.reset      = reset;
    this.resetBlock = resetTo;
    console.log('Reset To block: '+resetTo,this.resetBlock);
    this.isRoot     = null;
    this.status     = 'starting';
    this.branchId   = branchId;
    this.branchIp   = branchIp;
    this.bankWallet = new MkyWallet(branchId,branchIp,'banker',con);
    this.net        = branchNetwork;
    this.banks      = new BranchList(branchNetwork);
    this.banks.getBranchList();
    this.net.broadcast("Hello Bit Monky Miners");
    this.chain        = new MkyBlockChainMgr(this,con);
    this.group        = new MkyBankGroupMgr(this);
    this.wcon         = new MkyWebConsole(this.net,con,this);
    this.tranBuffer   = [];
    this.blockCtr     = [];
    this.maxBlockSize = null;
    this.firstBlock   = null;
    this.init();
    this.rollover     = null;
    this.logsRotating = null;
    this.rLogTimer    = null;
    this.rotateTransLog(1);
    this.startLogRotations();
  }
  async init(){
    if (this.reset)
      if (this.reset == 'rebuild')
        await this.reBuildDb(this.resetBlock);
      else
        await this.resetDb(this.resetBlock);

    this.firstBlock = new MkyBlock();
    this.maxBlockSize = this.firstBlock.maxBlockSize(0,this.maxBlockSize);

    for (var btype of bcTypes){
      const block = await this.checkLastBlockNbr(btype);
      var blockCtr = {
        nbr  : block.nbr +1,
        nRec : block.nRec + 0,

      }
      this.blockCtr.push(blockCtr);
    }
    console.log(this.blockCtr);

    if (this.reset && this.isRoot){
      const goldRate = await this.getGoldRate();
      const payment = this.bankWallet.makePaymentRec(this.firstBlock.minerReward(),'BMiner Reward',goldRate,0,'tblGoldTrans');
      const bTran   = new RegTransaction(payment.trans,con,payment.sig,this.getBlockCtr('tblGoldTrans'),this.maxBlockSize);
      bTran.confirmAndSave();
      console.log('create first transaction',this.firstTran);
    }
  }
  getGoldRate(){
    return new Promise( (resolve,reject)=>{
      const https = require('https');

      const pmsg = {msg : 'sendGoldRate'}
      const data = JSON.stringify(pmsg);

      const options = {
        hostname : 'www.bitmonky.com',
        port     : 443,
        path     : '/whzon/bitMiner/getGoldRate.php',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': data.length
        }
      }
      const req = https.request(options, res => {
        var rdata = '';
        res.on('data', d => {
          console.log(d);
          rdata += d;
        });
        res.on('end',()=>{
          var reply = null;
          console.log('getGold Rate returned',rdata);
          try {reply = JSON.parse(rdata);}
          catch(err) {reply = {mkyRate:0.0};}
          resolve(reply.mkyRate);
        });
      });

      req.on('error', error => {
        console.error(error)
        resolve(0.0);
      });

      req.write(data);
      req.end();
    });
  }
  getBlockCtr(type){
    for (var bc of this.blockCtr)
     if (bc.type == type);
       return bc;
  }
  resetBlockCtrs(){
    return new Promise( async (resolve,reject)=>{
      this.blockCtr = [];
      for (var btype of bcTypes){
        const block = await this.checkLastBlockNbr(btype);
        var blockCtr = {
          nbr  : block.nbr,
          nRec : block.nRec + 0,
          type : btype,
          maxBlockSize : this.maxBlockSize
        }
        this.blockCtr.push(blockCtr);
      }
      resolve (true);
    });
  }
  reSumerizeDB(){
    return new Promise( (resolve,reject)=>{
      console.log('reload day and month sum log');
      var SQL  = "insert into tblGoldTranDaySum (gtdsDate,gtdsGoldType,gtdsSource,gtdsTycTax, ";
      SQL += "gtdsAmount,gtdsGoldRate,gtdsMUID) ";
      SQL += "SELECT date(gtlDate),gtlGoldType,gtlSource,sum(gtlTycTax),sum(gtlAmount) ";
      SQL += ",avg(gtlGoldRate),gtlMUID ";
      SQL += "FROM tblGoldTranLog ";
      SQL += "group by date(gtlDate),gtlMUID,gtlGoldType,gtlSource,gtlSrcID";
      con.query(SQL, (err, result, fields)=>{
        if (err) {console.log(err);resolve(false);return;}
        else console.log( "\nSummerized Days transactions...\n");
        SQL  = "insert into tblGoldTranMonthSum (gtmsDate,gtmsGoldType,gtmsSource,gtmsTycTax, ";
        SQL += "gtmsAmount,gtmsGoldRate,gtmsMUID) ";
        SQL += "SELECT concat(DATE_FORMAT(gtlDate,'%Y-%m'),'-01'),gtlGoldType,gtlSource,sum(gtlTycTax),sum(gtlAmount) ";
        SQL += ",avg(gtlGoldRate),gtlMUID ";
        SQL += "FROM tblGoldTranLog ";
        SQL += "where date(gtlDate) <= DATE(NOW() - INTERVAL 1 MONTH) ";
        SQL += "group by year(gtlDate),month(gtlDate),gtlMUID,gtlGoldType,gtlSource,gtlSrcID";
        con.query(SQL, (err, result, fields)=>{
          this.rollover  = false;
          if (err) {console.log(err);resolve(false);return;}
          else console.log( "\nSummerized Month transactions...\n");
          resolve(true);
        });
      });
    });
  }
  reBuildFromBlocks(blockNbr){
    return new Promise( async (resolve,reject)=>{
      var cid = 1;
      const ctype = 'tblGoldTrans';
      console.log('\nVERIFY CHAIN '+cid,ctype);
      var branchId = this.branchId;
      var nblocks = await this.chain.getChainHeight(ctype);
      console.log('rebuild nblocks is: ',nblocks);
      var tbl = this.type;

      for (var nblock = blockNbr + 1; nblock <= nblocks;nblock++){
        var SQL = "select blockNbr,blockHash,blockPrevHash,blockNOnce,blockTimestamp,blockDifficulty,blockMinerID,blockHashTime,bchaBranchID,";
        SQL += "tranBlockData from mkyBlockC.tblmkyBlocks ";
        SQL += "inner join mkyBlockC.tblmkyBlockTrans on tranBlockChainID = blockChainID and tranBlockID = blockNbr ";
        SQL += "inner join mkyBlockC.tblmkyBlockChain on blockChainID = bchaID ";
        SQL += "where tranBlockID = "+nblock;
        const rebuild = true;
        //console.log('\n',SQL);
        await this.chain.verifyBlock(SQL,ctype,nblock,branchId,cid,rebuild);
      }
      console.log('Blocks Verified and Restored: '+nblock+' cid: '+cid,this.chainCtr);
      resolve('ok')
    });
  }
  reBuildDb(blockNbr=0){
    if (!blockNbr) blockNbr = 0;
    this.status = 'rebuilding database';
    console.log('starting rebuild at:',blockNbr);
    return new Promise( async (resolve,reject)=>{
      var SQL = "";
      SQL  = "truncate table mkyBank.tblGoldTranDaySum; ";
      SQL += "truncate table mkyBank.tblGoldTranMonthSum; ";
      SQL += "delete from mkyBank.tblGoldTrans        where gtrnBlockID > "+blockNbr+"; ";
      SQL += "delete from mkyBank.tblGoldTranLog      where gtlBlockID > "+blockNbr+"; ";

      con.query(SQL, async (err, result, fields)=>{
        if (err) {console.log(err);reject(err);this.status='Rebuild Failed';}
        else {
          await this.reBuildFromBlocks(blockNbr);
          this.reSumerizeDB();
          resolve("OK");
          this.status = 'Online';
        }
      });
    });
  }
  resetDb(blockNbr=null){
    return new Promise( (resolve,reject)=>{
      var SQL = "";
      if (!blockNbr){
        SQL = "truncate table mkyBank.tblGoldTranDaySum; ";
        SQL += "truncate table mkyBank.tblGoldTranMonthSum; ";
        SQL += "truncate table mkyBank.tblGoldTrans; ";
        SQL += "truncate table mkyBank.tblGoldTranLog;";
        SQL += "truncate table mkyBank.tblmkyWallets;";
        SQL += "truncate table mkyBlockC.tblmkyBlocks;";
        SQL += "truncate table mkyBlockC.tblmkyBlockTrans;";
      }
      else {
        SQL  = "truncate table mkyBank.tblGoldTranDaySum; ";
        SQL += "truncate table mkyBank.tblGoldTranMonthSum; ";
        SQL += "delete from mkyBank.tblGoldTrans        where gtrnBlockID > "+blockNbr+"; ";
        SQL += "delete from mkyBank.tblGoldTranLog      where gtlBlockID > "+blockNbr+"; ";
        SQL += "delete from mkyBlockC.tblmkyBlocks      where blockNbr > "+blockNbr+"; ";
        SQL += "delete from mkyBlockC.tblmkyBlockTrans  where tranBlockID > "+blockNbr+"; ";
      }
      con.query(SQL, async (err, result, fields)=>{
        if (err) {console.log(err);reject(err);}
        else {
          this.reSumerizeDB();
          resolve("OK");
        }
      });
    });
  }
  handleXhrError(j){
    if (!j.msg)
      return;
    const msg = j.msg;
    if (msg.req == 'sendStatus'){
      var node = {
        ip : j.toHost,
        status : 'offline',
        lastMsg : null
      }
      this.group.updateGroup(node);
      return;
    }
  }
  handleReq(res,j){

   //console.log('banker status',this.status);
   //console.log('Root status: ',this.isRoot);
    if (j.req == 'gotUAddMe'){
      this.group.addPeer(j.me);
      this.net.endRes(res,'');
      return true;
    }
    if (j.req == 'sendStatus'){
      this.group.me.status = this.status;
      this.net.endRes(res,'{"statusUpdate":'+JSON.stringify(this.group.me)+'}');
      return true;
    }
    if (j.req == 'changeBankStatus'){
      this.group.updateGroup(j.me);
      this.net.endRes(res,'');
      return true;
    }
    if (this.chain.handleReq(res,j)){
      return true;
    }
    if (this.banks.handleReq(res,j)){
      return true;
    }
    if (!this.isRoot && this.status != 'Online'){
      this.net.endRes(res,'');
      return true;
    }
    if (j.req == 'bitBalance'){
      this.getBalance(j,res);
      return true;
    }
    if (j.req == 'sendLastTick'){
      this.sendLastTick(res,j.type);
      return true;
    }
    if (j.req == 'sendBlockNbr'){
      this.sendBlockNbrRes(res,j.type,j.blockNbr);
      return true;
    }
    if (j.req == 'sendChainHeight'){
      this.sendChainHeight(res,j.type,j.myHeight);
      return true;
    }
    if (j.req == 'sendTrans'){
      this.sendLastTransaction(res,j.lastTimeStamp);
      return true;
    }
    if (j.req == 'sendMissingTrans'){
      this.sendMissingTrans(res,j.keys);
      return true;
    }
    if (j.req == 'sendWallets'){
     //console.log('got req sendWallets');
      this.sendLastWallets(res,j.lastWalMUID);
      return true;
    }
    if (j.req == 'sendBlock'){
      this.sendBlockRes(res,bcTypes[getRandomInt(2)]);
      return true;
    }
    return false;
  }
  handleReply(j){
   //console.log('\n====================\nmkyBanker reply handler',j);
    if (j.statusUpdate){
      this.group.updateGroup(j.statusUpdate);
      return;
    }
    if (this.chain.handleReply(j))
      return;

   //console.log('\nNo Bank Reply Handler Found For: ',j);
  }
  handleBCast(j){
    if (!j.msg.to) {return;}
    if (j.msg.to == 'bankers')
      if (j.msg.branch){
        if (j.msg.branch == this.branchId)
          this.procBranchReq(j.msg,j.remIp);
      }
      else {
        this.procBankersReq(j.msg,j.remIp);
      }

    if (j.branchId != this.branchId){return};
  }
  procBranchReq(msg,to){
    if (msg.joinGroup){
      this.group.addPeer(msg.joinGroup);
      this.group.replyGotUAddMe(to);
      this.group.listGroup();
      return;
    }

    if (this.status != 'Online'){
      console.log('Service Not Ready... buffering Transactions');
      //if (msg.procBitMonkTran){
      this.bufferTransactions(msg,to);
      //}
      return;
    }
    if (msg.blockConf){
      this.confirmNewBlock(msg.blockConf,msg.payment);
      return;
      //this.net.sendMsg(to,req);
    }
    if (msg.createAcc){
      var wal = msg.createAcc
      this.createAccount(wal.pubKey,wal.MUID,to);
      return;
    }
    if (msg.procBitMonkTran){
      this.procBitMonkTran(msg);
      return;
    }
  }
  selectTyBreaker(blockId,type,chainId,MUID){
    return new Promise( (resolve,reject)=>{
      var typeSearch = " gtrnQApp = '"+type+"' ";
      if (type == 'tblGoldTrans' || type == 'tblGoldTranLog')
        typeSearch = " (gtrnQApp = 'tblGoldTrans' or gtrnQApp = 'tblGoldTranLog') ";

      var SQL = "select gtrnID,gtrnMUID from mkyBank.tblGoldTrans where gtrnSource = 'BMiner Reward' and gtrnSrcID = "+blockId+" and "+typeSearch;
      con.query(SQL , function (err, result,fields) {
        if (err){console.log(err),process.exit();resolve(false);}
        else {
          if (result.length == 0){
            resolve(false);
          }
          else {
            if (MUID < result[0].gtrnMUID){
              resolve(true);
              this.removeBlockLoser(blockID,type,chainId,result[0].gtrnID);
            }
            else
              resolve(false);
          }
        }
      });
    });
  }
  removeBlockLoser(blockID,type,chainId,gtrnID){
    var SQL = "delete from mkyBank.tblGoldTrans where gtrnID = "+gtrnID;
    con.query(SQL , async (err, result,fields)=>{
      if (err){console.log(err);process.exit();}
    });
    SQL = "delete from mkyBlockC.tblBlocks where blockChainID = "+chainId+" blockNbr = "+blockID;
    con.query(SQL , async (err, result,fields)=>{
      if (err){console.log(err);process.exit();}
    });
    SQL = "delete from mkyBlockC.tblBlockTrans where tranBlockChainID = "+chainId+" tranBlockNbr = "+blockID;
    con.query(SQL , async (err, result,fields)=>{
      if (err){console.log(err);process.exit();}
    });
  }
  async checkWinnerStatus(conf,payMUID){
    const chainId = await this.getBlockChainId(conf.type);
    return new Promise( (resolve,reject)=>{

      var SQL =  "select blockTimestamp,blockDifficulty  from mkyBlockC.tblmkyBlocks ";
      SQL += "where blockNbr = "+conf.blockID+" and blockChainID = "+chainId;
      const bank = this;
      con.query(SQL , async (err, result,fields)=>{
        if (err){console.log(err),process.exit();resolve(false);}
        else {
          if (result.length == 0){
            resolve(true);
          }
          else {
            const rec = result[0];
            if (conf.dif < rec.blockDifficulty)
              resolve (false)
            else {
              if(conf.timestamp > rec.blockTimestamp){
                resolve (false);
              }
              else {
                const tyBreak = await bank.selectTyBreaker(conf.blockID,conf.type,chainId,payMUID)
                resolve(tyBreak);
              }
            }
          }
        }
      });
    });
  }
  async confirmNewBlock(conf,payment){
    const chainHeight = await this.chain.getChainHeight(conf.type);
    console.log('conf',conf);
    console.log('payment',payment);
    if (conf.blockID < chainHeight || conf.blockID > chainHeight +1){
      console.log('Block '+conf.blockID+' Rejected type '+conf.type,chainHeight);
      return;
    }
    var confKey = {
       start : conf.firstRec.gtlDate + conf.firstRec.syncKey,
       end   : conf.lastRec.gtlDate + conf.lastRec.syncKey
    }
    var SQL =  this.buildBlockSQL(conf.blockID,conf.type,confKey);
    var bank = this;
    const isWin = await this.checkWinnerStatus(conf,payment.gtlMUID);
    console.log('\n---------------\nwinner status is -->',isWin);
    if (!isWin){
      return;
    }
    con.query(SQL , async (err, result,fields)=>{
      if (err){
        console.log(err);
      }
      else {
        var trans = [];
        const dbres = Object.keys(result);
        dbres.forEach(function(key) {
          var tRec = result[key];
          trans.push(tRec);
        });
        var transStr = JSON.stringify(trans);
        trans = JSON.parse(transStr);
        console.log('confirming block '+ conf.blockID,trans);
        var prevHash = await bank.getBlockPreviousHash(conf.blockID -1);
        var chainId = await bank.getBlockChainId(conf.type);
        var block = new MkyBlock(conf.timestamp,conf.blockID,trans,prevHash,null,conf.branchId,conf.type,chainId);
        var minerID = 0;

        if (block.checkSolution(conf.diff,conf.nonce,conf.timestamp,conf.hash)){
          conf.chainId = chainId;
          var sres = await bank.storeBlockChainRec(conf,transStr,prevHash,minerID);
          if (sres){
            console.log('block confirmed!');
            await bank.reBlockTransactions(conf);
            bank.chain.calibrateHashRate(conf.type,conf.blockID);
            const nQued = await this.getNQTransactions();
            this.maxBlockSize = this.firstBlock.maxBlockSize(nQued,this.maxBlockSize);
            bank.makeMinerPayment(conf,payment);
          }
          else
            console.log('data error saving to database');
        }
        else {
          console.log('block confirmatin FAIL',trans.length);
        }
      }
    });
  }
  getNQTransactions(){
    return new Promise( (resolve,reject)=>{
      var SQL =  "select count(*)nRec from tblGoldTrans where gtrnBlockID is null ";
      con.query(SQL , (err, result,fields)=>{
        if (err){console.log(err); resolve(0); }
        else
          resolve(result[0].nRec);
      });
    });
  }
  blockRemainingTransaction(nextBlockID){
    console.log('blockRemaining',nextBlockID);
    var SQL = "Select count(*)nRec from tblGoldTrans where gtrnBlockConfirmed is null and gtrnBlockID is null ";
    con.query(SQL, (err, result, fields)=>{
      if (err) {console.log(err);}
      else {
        if (result[0].nRec > 0){
          SQL = "update tblGoldTrans set gtrnBlockID = "+nextBlockID+" where gtrnBlockConfirmed and gtrnBlockID is null ";
          SQL += "order by gtrnDate desc,gtrnSyncKey desc limit "+this.maxBlockSize;
          con.query(SQL, (err, result, fields)=>{
            if (err) {console.log(err);}
            this.blockRemainingTransaction(nextBlockID+1);
          });
        }
      }
    });
  }
  markConfirmedTrans(confKey,key,bc,blockID){
    return new Promise( (resolve,reject)=>{
      const nextBlockID = bc.nbr +1;
      var SQL = "update tblGoldTrans set gtrnBlockID = null where gtrnBlockConfirmed is null";
      con.query(SQL, (err, result, fields)=>{
        if (err) {console.log(err); resolve(false);}
        else {
          SQL = "update tblGoldTrans set gtrnBlockID = "+blockID+", gtrnBlockConfirmed = now() where gtrnBlockConfirmed is null and "+key;
          SQL += "order by gtrnDate desc,gtrnSyncKey desc limit "+this.maxBlockSize;
          con.query(SQL, async (err, result, fields)=>{
            if (err) {console.log(err); resolve(false);}
            bc.nbr++;
            bc.nRec = 0;
            //console.log('bc is now',this.blockCtr);
            //this.blockRemainingTransaction(nextBlockID);
            resolve(true);
          });
        }
      });
    });
  }
  reBlockTransactions(conf){
    return new Promise( async (resolve,reject)=>{
      //console.log('reBlockTransactions beging');
      var confKey = {
         start : conf.firstRec.gtlDate + conf.firstRec.syncKey,
         end   : conf.lastRec.gtlDate + conf.lastRec.syncKey
      }
      const search = " concat(gtrnDate,gtrnSyncKey) >= '"+confKey.start+"' and concat(gtrnDate,gtrnSyncKey) <=  '"+confKey.end+"' ";
      await this.resetBlockCtrs();
      const bc = this.getBlockCtr('tblGoldTrans');
      await this.markConfirmedTrans(confKey,search,bc,conf.blockID);
      resolve(true);
    });
  }
  makeMinerPayment(conf,payment){
   //console.log('Banker: payment.sig',payment.sig);
    const bTran = new RegTransaction(payment.trans,con,payment.sig,this.getBlockCtr('tblGoldTrans'),this.maxBlockSize);
    bTran.confirmAndSave();

    var breq = {
      to : 'miners',
      stop : 'stop mining',
      block : conf
    }
    this.net.broadcast(breq);
  }
  getBlockPreviousHash(id){
    return new Promise( (resolve,reject)=>{
      var SQL = "select blockHash from  mkyBlockC.tblmkyBlocks where blockNbr = "+id;
      con.query(SQL, async function (err, result, fields) {
        if (err) reject(err);
        else {
          if (result.length  > 0) {
            var rec = result[0];
            resolve(rec.blockHash);
          }
          else
            resolve('Genissis Block');
        }
      });
    });
  }
  procBankersReq(msg,to){
    //console.log('\nProcBankersReq to '+ to,msg,to);
    if (msg.send == 'blistInfo'){
      var req = {
        req : 'bcReply',
        blistInfo : {
          ip  : this.branchIp,
          id  : this.branchId
        }
      }
      //console.log('response to banker node: ',req.blistInfo);
      this.net.sendMsg(to,req);
    }
  }
  bufferTransactions(j,to){
     const req = {
       msg : j,
       to : to
     }
     this.tranBuffer.push(req);
  }
  flushTranBuffer(){
    console.log('start flush tranBuffer');

    for (var j of this.tranBuffer){
      this.procBranchReq(j.msg,j.to);
      console.log('Flushing Transaction Buffer',j);
    }
    this.tranBuffer = [];
  }
  procBitMonkTran(j){
     const bTran = new MkyTransaction(mkyPubKey,j.tran,j.sig,j.tdate,this.getBlockCtr('tblGoldTrans'));
    //console.log('bitmonky transaction isValid: ',bTran.isValid());
    //console.log(j.tran);
    //console.log(j.sig);
    //console.log(mkyPubKey);
    //console.log(j.tdate);
     bTran.saveTransaction();
     //process.exit();
  }
  isToday(d){
    let today = new Date(Date.now() -1000*3600*5).toISOString().slice(0, 10);
    if (today == d.slice(0,10))
      return true;
    else
      return false;
  }
  getBalance(j,res){
      const mUID = j.mUID;
      var SQL  = "select R.type ,sum(R.tax)tax,sum(R.amt)amt from ( ";
      SQL += "SELECT gtrnGoldType type,sum(gtrnTycTax)tax,sum(gtrnAmount)amt FROM tblGoldTrans ";
      SQL += "where gtrnMUID = '"+mUID+"' and DATE(gtrnDate) = DATE(NOW()) ";
      SQL += "group by gtrnGoldType ";
      SQL += "union ";
      SQL += "SELECT gtdsGoldType,sum(gtdsTycTax),sum(gtdsAmount)FROM tblGoldTranDaySum ";
      SQL += "where gtdsMUID = '"+mUID+"' and month(gtdsDate) = month(NOW()) and year(gtdsDate) = year(NOW()) ";
      SQL += "group by gtdsGoldType ";
      SQL += "union ";
      SQL += "SELECT gtmsGoldType,sum(gtmsTycTax),sum(gtmsAmount)FROM tblGoldTranMonthSum ";
      SQL += "where gtmsMUID = '"+mUID+"' ";
      SQL += "group by gtmsGoldType)R ";
      SQL += "group by R.type";

      con.query(SQL , (err, result,fields)=>{
        if (err){
          console.log(err);
          this.net.endRes(res,JSON.stringify(err));
        }
        else {
          var bal = 0.0;
          Object.keys(result).forEach(function(key) {
            var row = result[key];
            bal = bal + row.amt;
          });
          this.net.endRes(res,'{"bankBalance":' + bal + '}');
        }
      });
  }
  createAccount(pubKey,wMUID,to){
    var net = this.net;
    let SQL = "select mwalID from tblmkyWallets where mwalMUID = '" + wMUID + "'";
    let branchId = this.branchId;
    console.log('create wallet acc send result to ',to);
    con.query(SQL, function (err, result, fields) {
      if (err)
        net.sendMsg(to,{bankResult:{newWallet:null,error:"database failed"}});
      else {
        if (result.length)
          net.sendMsg(to,{bankResult:{newWallet: result[0].mwalID ,status:"onFile"}});
        else {
          let SQL = "insert into tblmkyWallets (mwalPubKey,mwalGBranchID,mwalDate,mwalMUID) ";
          SQL += "values('" + pubKey + "'," + branchId + ",now(),'" + wMUID + "')";
          con.query(SQL, function (err, result, fields) {
            if (err)
              net.sendMsg(to,{bankResult:{newWallet:null,error:"database failed: "+SQL}});
            else {
              let SQL = "select mwalID from tblmkyWallets where mwalMUID = '"+ wMUID +"'";
              con.query(SQL, function (err, result, fields) {
                if (err)
                  net.sendMsg(to,{bankResult:{newWallet:null,error:"database failed: "+SQL}});
                else {
                  net.sendMsg(to,{bankResult:{newWallet: result[0].mwalID }});
                }
              });
            }
          });
        }
      }
    });
  }
  sendLastWallets(res,lastWalMUID){
    var SQL = "select * from tblmkyWallets where mwalMUID > '"+lastWalMUID+"' order by mwalMUID ";
    var SQL = "select * from tblmkyWallets ";
   //console.log(SQL);
    con.query(SQL , (err, result,fields)=>{
      if (err){
        this.net.endRes(res,'"blockErr":' + JSON.stringify(err)+'}');
       //console.log('db error',SQL);
      }
      else {
        var trans = [];
        const dbres = Object.keys(result);
        dbres.forEach(function(key) {
          var tRec = result[key];
          trans.push(tRec);
        });
        if (trans.length == 0){
          this.net.endRes(res,'{"result":"No More Wallets To Send."}');
         //console.log('no wallets to send');
        }
        else {
          var myResponse = {
            bLastWallets : trans
          }
         //console.log('sending wallets',myResponse);
          this.net.endRes(res,JSON.stringify(myResponse));
        }
      }
    });
  }
  sendMissingTrans(res,keys){
    //const key = lastTimeStamp.date+lastTimeStamp.key;
    console.log('got sendMissingTrans',keys);
    this.net.endRes(res,'{"missingTrans":[]}');
    return;
    /***********************************
      NEEDS To Be Writen ***************
    ************************************
    */
    var SQL = "select gtrnDate,gtrnGoldType,gtrnSource,cast(gtrnSrcID as char)gtrnSrcID,cast(gtrnTycTax as char)gtrnTycTax,";
    SQL += "cast(gtrnAmount as char)gtrnAmount,cast(gtrnCityID as char)gtrnCityID,";
    SQL += "cast(gtrnTaxHold as char)gtrnTaxHold,cast(gtrnGoldRate as char)gtrnGoldRate,gtrnSyncKey,gtrnQApp,gtrnMUID,gtrnSignature ";
    SQL += ",cast(gtrnBlockID as char)gtrnBlockID from tblGoldTrans where concat(gtrnDate,gtrnSyncKey) <  '"+key+"' order by gtrnDate,gtrnSyncKey ";
    con.query(SQL , (err, result,fields)=>{
      if (err){
        this.net.endRes(res,'"blockErr":' + JSON.stringify(err)+'}');
      }
      else {
        var trans = [];
        const dbres = Object.keys(result);
        dbres.forEach(function(key) {
          var tRec = result[key];
          trans.push(tRec);
        });
        if (trans.length == 0)
          this.net.endRes(res,'{"result":"No Transactions To Send Right Now."}');
        else {
          var myResponse = {
            bLastTrans : trans
          }
          this.net.endRes(res,JSON.stringify(myResponse));
        }
      }
    });
  }
  sendLastTransaction(res,lastTimeStamp){
    const key = lastTimeStamp.date+lastTimeStamp.key;
    var SQL = "select gtrnDate,gtrnGoldType,gtrnSource,cast(gtrnSrcID as char)gtrnSrcID,cast(gtrnTycTax as char)gtrnTycTax,";
    SQL += "cast(gtrnAmount as char)gtrnAmount,cast(gtrnCityID as char)gtrnCityID,";
    SQL += "cast(gtrnTaxHold as char)gtrnTaxHold,cast(gtrnGoldRate as char)gtrnGoldRate,gtrnSyncKey,gtrnQApp,gtrnMUID,gtrnSignature ";
    SQL += ",cast(gtrnBlockID as char)gtrnBlockID from tblGoldTrans where concat(gtrnDate,gtrnSyncKey) >  '"+key+"' order by gtrnDate,gtrnSyncKey ";
    console.log(SQL);
    con.query(SQL , (err, result,fields)=>{
      if (err){
        this.net.endRes(res,'"blockErr":' + JSON.stringify(err)+'}');
      }
      else {
        var trans = [];
        const dbres = Object.keys(result);
        dbres.forEach(function(key) {
          var tRec = result[key];
          trans.push(tRec);
        });
        if (trans.length == 0){
          console.log('sendLastTrans','no trans to send');
          this.net.endRes(res,'{"result":"No Transactions To Send Right Now."}');
        }
        else {
          var myResponse = {
            bLastTrans : trans
          }
          console.log('sending transactions'+res,trans.length);
          this.net.endRes(res,JSON.stringify(myResponse));
        }
      }
    });
  }
  async sendChainHeight(res,type,reqHeight){
    const myHeight = await this.chain.getChainHeight(type);
    const hash     = await this.chain.getChainHash(type);
    const chainId  = await this.getBlockChainId(type);
    var rHeight = null;
    var myResponse = {
      rqChainHeight : myHeight,
      rqChainHash   : hash,
      chainId       : chainId,
      forType       : type,
      host          : this.net.network.me.ip
    }
    this.net.endRes(res,JSON.stringify(myResponse));
  }
  sendLastTick(res,type){
    var myResponse = {
      hrTicker : this.chain.hrTicker,
      type     : type
    }
    console.log('sending lastTick ',myResponse);
    this.net.endRes(res,JSON.stringify(myResponse));
  }
  sendBlockNbrRes(res,type,blockNbr){
       var SQL = "select blockHashTime,bchaBranchID,blockHash,blockPrevHash,blockNOnce,blockTimestamp,blockMinerID,blockDifficulty,tranBlockData ";
       SQL += " from mkyBlockC.tblmkyBlocks ";
       SQL += "inner join mkyBlockC.tblmkyBlockChain on blockChainID = bchaID ";
       SQL += "inner join mkyBlockC.tblmkyBlockTrans on tranBlockChainID = blockChainID and tranBlockID = blockNbr ";
       SQL += "where bchaSrcTable = '"+type+"' and blockNbr = "+blockNbr;
       con.query(SQL , (err, result,fields)=>{
         if (err){
           this.net.endRes(res,'"blockErr":' + JSON.stringify(err)+'}');
           process.exit(SQL);
         }
         else {
           if (result.length == 0){
             var reply = {
               bInfo : {
                 blockNbr : null
               }
             }
             console.log('sending block info',reply);
             this.net.endRes(res,JSON.stringify(reply));
           }
           else {
             const tRec = result[0];

             var myResponse = {
               bInfo : {
                 blockNbr   : blockNbr,
                 blockID    : blockNbr,
                 type       : type,
                 trans      : JSON.parse(tRec.tranBlockData),
                 nonce      : tRec.blockNOnce,
                 hash       : tRec.blockHash,
                 prevHash   : tRec.blockPrevHash,
                 timestamp  : tRec.blockTimestamp,
                 branchId   : tRec.bchaBranchID,
                 minerId    : tRec.blockMinerID,
                 diff       : tRec.blockDifficulty,
                 hrTicker   : this.chain.hrTicker,
                 hashTime   : tRec.blockHashTime
               }
             }
             console.log('sending block ',blockNbr);
             this.net.endRes(res,JSON.stringify(myResponse));
           }
         }
       });
  }
  async sendBlockRes(res,type){
    console.log('sending block of type ',type);
    const nb = await this.getNewBlockNbr(type);

    if (!nb){
      this.net.endRes(res,'{"result":"No Blocks To Mine Right Now."}');
      return;
    }
    const SQL = this.buildBlockSQL(nb.number,type);
    con.query(SQL , (err, result,fields)=>{
      if (err){
        console.log(err);
        this.net.endRes(res,'"blockErr":' + JSON.stringify(err)+'}');
      }
      else {
        var trans = [];
        const dbres = Object.keys(result);
        dbres.forEach(function(key) {
          var tRec = result[key];
          trans.push(tRec);
        });
        console.log('block found'+ trans.length,nb);
        if (trans.length == 0){
          //console.log(SQL);
          this.net.endRes(res,'{"result":"No Blocks To Mine Right Now."}');
        }
        else
          this.net.endRes(res,'{"newBlock":true,"trans":'+ JSON.stringify(trans) + ',"bInfo":'+JSON.stringify(nb)+'}');
      }
    });

  }
  getBlock(blockId,type){
    return new Promise( (resolve,reject)=>{
      const SQL = this.buildBlockSQL(blockId,type);
      con.query(SQL , function (err, result,fields) {
        if (err){
          reject(err);
        }
        else {
          var trans = [];
          const res = Object.keys(result);
          res.forEach(function(key) {
            var tRec = result[key];
            trans.push(tRec);
          });
          resolve( JSON.stringify(trans));
        }
      });
    });
  }
  storeBlockChainRec(conf,trans,prevHash,minerID){
    var bank = this;
    return new Promise( (resolve,reject)=>{
      var SQL = "select count(*)nBlocks from mkyBlockC.tblmkyBlocks where blockChainID = "+conf.chainId+" and blockNbr = "+conf.blockID;
      con.query(SQL, async function (err, result, fields) {
        if (err) {console.log(err);reject(false);}
        else {
          var tRec = result[0];
          if (tRec.nBlocks == 0){
            SQL = "insert into mkyBlockC.tblmkyBlocks (blockNbr,blockHash,blockPrevHash,blockNOnce,blockTimestamp,blockChainID,";
            SQL += "blockMinerID,blockDifficulty,blockHashTime) ";
            SQL += "values ("+conf.blockID+",'"+conf.hash+"','"+prevHash+"',"+conf.nonce+","+conf.timestamp+","+conf.chainId;
            SQL += ","+minerID+","+conf.diff+","+Date.now()+")";
            con.query(SQL, async function (err, result, fields) {
              if (err) {console+log(err);reject(false);}
              else {
                var res = await bank.storeBlockTransData(trans,conf.blockID,conf.chainId);
                resolve(res);
              }
            });
          }
          else {
           //console.log('Block Already Exists');
            resolve(false);
          }
        }
      });
    });
  }
  storeBlockTransData(transactions,blockID,chainId){
    return new Promise( (resolve,reject)=>{
      var SQL = "Select count(*)nRec from mkyBlockC.tblmkyBlockTrans where tranBlockID = "+blockID+" and tranBlockChainID = "+chainId;
      con.query(SQL, async function (err, result, fields) {
        if (err) {console.log(err);reject(false);}
        else {
          var tRec = result[0];
          if (tRec.nRec == 0){
            SQL  = "insert into mkyBlockC.tblmkyBlockTrans (tranBlockID,tranBlockChainID,tranBlockData) ";
            SQL += "values("+blockID+","+chainId+",'"+addslashes(transactions)+"')";
            con.query(SQL, async function (err, result, fields) {
              if (err) {console.log(err);reject(false);}
              else {
                resolve(true);
              }
            });
          }
          else {
            SQL  = "update mkyBlockC.tblmkyBlockTrans set tranBlockData = '"+addslashes(transactions)+"'";
            SQL += "where tranBlockID = "+blockID+" and tranBlockChainID = "+chainId;
            con.query(SQL, async function (err, result, fields) {
              if (err) {console.log(err);reject(false);}
              else resolve(true);
            });
          }
        }
      });
    });
  }
  checkLastBlockNbr(type){
    return new Promise( (resolve,reject)=>{
      var SQL = null;
      if (type == 'tblGoldTranLog' || type == 'tblGoldTrans' ){
        SQL = "SELECT count(*)nRec,gtrnBlockID blockNbr from tblGoldTrans group by gtrnBlockID ";
        SQL += "union ";
        SQL += "SELECT count(*), gtlBlockID from tblGoldTranLog group by gtlBlockID order by blockNbr desc limit 1";
      }
      if (type == 'tblGoldTranDaySum'){
        SQL = "SELECT count(*)nRec gtdsBlockNbr blockNbr from tblGoldTranDaySum group by gtdsBlockNbr order by gtdsBlockNbr desc limit 1";
      }
      if (type == 'tblGoldTranMonthSum'){
        SQL = "SELECT count(*)nRec gtdsBlockNbr blockNbr from tblGoldTranDaySum group by gtdsBlockNbr order by gtdsBlockNbr desc limit 1";
      }
      //console.log('last block check',SQL);
      con.query(SQL, async function (err, result, fields) {
        if (err) {console.log(err);reject(false);}
        else {
          if (result.length == 0)
            resolve({nRec:0,nbr:0});
          else {
            const rec = result[0];
            resolve({nRec:rec.nRec,nbr:rec.blockNbr});
          }
        }
      });
    });
  }
  buildBlockSQL(blockId,type,confKey=null){
    var searchA = '';
    var searchB = '';
    if (confKey){
      searchA = " and concat(gtrnDate,gtrnSyncKey) >= '"+confKey.start+"' and concat(gtrnDate,gtrnSyncKey) <=  '"+confKey.end+"' ";
      searchB = " and concat(gtlDate,syncKey) >= '"+confKey.start+"' and concat(gtlDate,syncKey) <=  '"+confKey.end+"' ";
    }
    var SQL = null;
    if (type == 'tblGoldTranLog' || type == 'tblGoldTrans' ){
      SQL = "SELECT gtlAmount,"+blockId+" gtlBlockID,gtlCityID,gtlDate,gtlGoldRate,gtlGoldType,gtlMUID,gtlQApp,gtlSignature,gtlSource";
      SQL += ",gtlSrcID,gtlTaxHold,gtlTycTax,syncKey ";
      SQL += "from tblGoldTranLog where gtlBlockID is null "+searchB+" ";
      SQL += "union ";
      SQL += "SELECT gtrnAmount,"+blockId+" gtrnBlockID,gtrnCityID,gtrnDate,gtrnGoldRate,gtrnGoldType,gtrnMUID,gtrnQApp,gtrnSignature,gtrnSource";
      SQL += ",gtrnSrcID,gtrnTaxHold,gtrnTycTax,gtrnSyncKey ";
      SQL += "from tblGoldTrans where gtrnBlockConfirmed is null and gtrnBlockID is null "+searchA+" order by gtlDate,syncKey limit "+this.maxBlockSize;
    }
    if (type == 'tblGoldTranDaySum'){
      SQL = "SELECT gtdsDate ,gtdsGoldType,gtdsSource,gtdsTycTax,gtdsAmount ";
      SQL += ",gtdsGoldRate,gtdsBlockNbr ";
      SQL += "from tblGoldTranDaySum where gtdsBlockNbr = "+blockId+" order by gtdsDate";
    }
    if (type == 'tblGoldTranMonthSum'){
      SQL = "SELECT gtmsDate ,gtmsGoldType,gtmsSource,gtmsTycTax,gtmsAmount ";
      SQL += ",gtmsGoldRate,gtmsBlockNbr ";
      SQL += "from tblGoldTranMonthSum where gtmsBlockNbr = "+blockId+" order by gtmsDate";
    }
    return SQL;
  }
  getBlockChainId(mySrc){
    var bank = this;
    if (mySrc == 'tblGoldTrans')
      mySrc = 'tblGoldTranLog';
    return new Promise( (resolve,reject)=>{
      var SQL = "SELECT bchaID FROM mkyBlockC.tblmkyBlockChain where bchaSrcTable = '"+mySrc+"'";
      con.query(SQL, function (err, result, fields) {
        if (err) {console.log(err);resolve(null);}
        else {
          if (result.length  > 0)
            resolve (result[0].bchaID);
          else
            resolve (null);
        }
      });
    });
  }
  getNewBlockNbr(mySrc){
    var bank = this;
    var oSrc = mySrc;
    if (mySrc == 'tblGoldTrans')
      mySrc = 'tblGoldTranLog';
    return new Promise( (resolve,reject)=>{
      var SQL = "SELECT bchaDifficulty,bchaID FROM mkyBlockC.tblmkyBlockChain where bchaSrcTable = '"+mySrc+"'";
      con.query(SQL, async (err, result, fields)=>{
        if (err) reject(err);
        else {
          if (result.length  > 0) {
            var rec = result[0];
            const rate = await this.getGoldRate();
            var newBlock = {
              difficulty : rec.bchaDifficulty + 0,
              chainId    : rec.bchaID,
              number     : null,
              prevHash   : 'Genissis Block',
              branchId   : bank.branchId,
              type       : oSrc,
              goldRate   : rate
            }
            SQL = "select count(*)nBlock from  mkyBlockC.tblmkyBlocks where blockChainID = "+newBlock.chainId;
            con.query(SQL, async function (err, result, fields) {
              if (err) reject(err);
              else {
                if (result.length  > 0) {
                  var rec = result[0];
                  var nBlock = rec.nBlock;
                  if (nBlock == 0){
                    newBlock.number = 1;
                    resolve(newBlock);
                  }
                  else {
                    SQL = "select blockNbr,blockHash from  mkyBlockC.tblmkyBlocks where blockChainID = "+newBlock.chainId+" Order by blockID desc limit 1";
                    con.query(SQL, async function (err, result, fields) {
                      if (err) reject(err);
                      else {
                        if (result.length  > 0) {
                          var rec = result[0];
                          newBlock.number = rec.blockNbr +1;
                          newBlock.prevHash = rec.blockHash;
                          resolve(newBlock);
                        }
                      }
                    });
                  }
                }
              }
            });
          }
          else {
            resolve(null);
          }
        }
      });
    });
  }
  //*****************************************************************
  // Start Rotations For the transaction log,day sum and month sum files
  //=================================================================
  startLogRotations(){
    schedule.scheduleJob('0 0 * * *', () => {
      var iDay = 1;
      console.log('Midnight Roll');
      this.rotateTransLog(iDay);
      if (this.rLogTimer)clearInterval(this.rLogTimer);
      this.rLogTimer = setInterval( ()=>{
        this.rotateTransLog(iDay);
      },120*1000);
    });
  }
  //*****************************************************************
  // Rotate Days Transactions to the log
  //=================================================================
  rotateTransLog(iDay){
    if (this.logsRotating)
      return;
    this.logsRotating = true;
    console.log("Start Main");
    const bank = this;

    let SQL = "select count(*)nRec from tblGoldTranLog where DATE(gtlDate) = DATE(NOW() - INTERVAL "+iDay+" DAY)";
    con.query(SQL, (err, result, fields)=>{
      if (err){this.logsRotating = false;console.log(err);}
      else {
        if (result[0].nRec == 0) {
          SQL = "SELECT count(*)nRec FROM tblGoldTrans where DATE(gtrnDate) = DATE(NOW() - INTERVAL "+iDay+" DAY)";
          con.query(SQL, async (err, result, fields)=>{
            if (err){this.logsRotating = false;console.log(err)}
            else {
              if(result[0].nRec > 0){
                this.rollover = true;
                SQL = "insert into tblGoldTranLog (gtlDate,gtlGoldType,gtlSource,gtlSrcID,gtlTycTax,gtlAmount,gtlCityID ";
                SQL += ",gtlTaxHold,gtlGoldRate,syncKey,gtlQApp,gtlMUID,gtlBlockID,gtlSignature) ";
                SQL += "SELECT gtrnDate,gtrnGoldType,gtrnSource,gtrnSrcID,gtrnTycTax,gtrnAmount,gtrnCityID ";
                SQL += ",gtrnTaxHold,gtrnGoldRate,gtrnSyncKey,gtrnQApp,gtrnMUID,gtrnBlockID,gtrnSignature ";
                SQL += "FROM tblGoldTrans where NOT (gtrnBlockID is null or gtrnBlockConfirmed is null)" ;
                SQL += " and  DATE(gtrnDate) = DATE(NOW() - INTERVAL "+iDay+" DAY)";
                console.log(SQL);
                con.query(SQL, (err, result, fields)=> {
                  if (err){this.logsRotating = false;console.log(err);}
                  else {
                    SQL = "delete FROM tblGoldTrans where DATE(gtrnDate) = DATE(NOW() - INTERVAL "+iDay+" DAY)";
                    con.query(SQL, (err, result, fields)=>{
                      if (err){this.logsRotating = false;console.log(err);}
                      else {
                        bank.doDaySum(iDay);
                        console.log('Done '+SQL);
                      }
                    });
                  }
                });
              }
              else {
                this.rollover = true;
                SQL = "select count(*)nRec FROM tblGoldTrans where DATE(gtrnDate) = DATE(NOW() - INTERVAL "+iDay+" DAY)";
                con.query(SQL, (err, result, fields)=>{
                  if (err) {this.logsRotating = false;console.log(err);}
                  else {
                    if (result[0].nRec > 0) {
                      SQL = "delete FROM tblGoldTrans where DATE(gtrnDate) = DATE(NOW() - INTERVAL "+iDay+" DAY)";
                      con.query(SQL, (err, result, fields)=> {
                        if (err) {this.logsRotating = false;console.log(err);}
                        else  console.log('Remove Undeleted Records  '+SQL);
                      });
                    }
                    bank.doDaySum(iDay);
                  }
                });
              }
            }
          });
        }
        else {
          this.logsRotating = false;
        }
      }
    });
  }
  //*****************************************************************
  // Create Days Sum of transactions
  //=================================================================
  doDaySum(iDay){
    if (!this.logsRotating)
      return;
    console.log('update day sum log');
    var SQL = "select count(*)nRec FROM tblGoldTranDaySum where  DATE(gtdsDate) = DATE(NOW() - INTERVAL "+iDay+" DAY)";
    con.query(SQL, (err, result, fields)=> {
      if (err) {console.log(err);this.logsRotating = false;this.rollover = false;}
      else {
        const rec = result[0];
        if (rec.nRec == 0) {
          SQL  = "insert into tblGoldTranDaySum (gtdsDate,gtdsGoldType,gtdsSource,gtdsTycTax, ";
          SQL += "gtdsAmount,gtdsGoldRate,gtdsMUID) ";
          SQL += "SELECT date_add(now(),interval -"+iDay+" day), ";
          SQL += "gtlGoldType,gtlSource,sum(gtlTycTax),sum(gtlAmount) ";
          SQL += ",avg(gtlGoldRate),gtlMUID ";
          SQL += "FROM tblGoldTranLog ";
          SQL += "where DATE(gtlDate) = DATE(NOW() - INTERVAL "+iDay+" DAY) ";
          SQL += "group by gtlMUID,gtlGoldType,gtlSource,gtlSrcID";
          con.query(SQL, (err, result, fields)=>{
            this.rollover  = false;
            if (err) {this.logsRotating = false;console.log(err);}
            else console.log( "\nSummerizing Days transactions...\n");
          });
        }
      }
      this.doMonthSum();
    });
  }
  //*****************************************************************
  // Create Month sum of transactions
  //=================================================================
  doMonthSum(){
    if (!this.logsRotating)
      return;
    console.log('update Month sum log');
    var SQL = "select count(*)nRec FROM tblGoldTranMonthSum where DATE(gtmsDate) = DATE(NOW() - INTERVAL 1 MONTH)";
    console.log(SQL)
    con.query(SQL, (err, result, fields)=>{
      if (err) {console.log(err); this.logsRotating = false;}
      else {
        const rec = result[0];
        if (rec.nRec == 0) {
          this.rollover = true;
          SQL  = "insert into tblGoldTranMonthSum (gtmsDate,gtmsGoldType,gtmsSource,gtmsTycTax, ";
          SQL += "gtmsAmount,gtmsGoldRate,gtmsMUID) ";
          SQL += "SELECT date_add(now(),interval -1 month), ";
          SQL += "gtlGoldType,gtlSource,sum(gtlTycTax),sum(gtlAmount) ";
          SQL += ",avg(gtlGoldRate),gtlMUID ";
          SQL += "FROM tblGoldTranLog ";
          SQL += "where month(gtlDate) = month(NOW() - INTERVAL 1 MONTH) ";
          SQL += "and year(gtlDate) = year(NOW() - INTERVAL 1 MONTH) ";
          SQL += "group by gtlMUID,gtlGoldType,gtlSource,gtlSrcID";
          con.query(SQL, (err, result, fields)=>{
            this.rollover = false;
            if (err){this.logsRotating = false;console.log(err);}
            else {
              this.logsRotating = false;
              console.log( "\nSummerizing Months transactions...\n");
            }
          });
        }
        else {
          this.logsRotating = false;
        }
      }
    });
  }
}

function sleep(ms){
    return new Promise(resolve=>{
        setTimeout(resolve,ms)
    })
}

module.exports.MkyTransaction = MkyTransaction;
module.exports.MkyBank  = MkyBank;
