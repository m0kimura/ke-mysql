'use strict';
const Ms=require('mysql');
const keBase=require('ke-base');
module.exports=class keMysql extends keBase {
  constructor(op, table) {
    super(op, table);
  }
  /**ok
   * データベースのオープン
   * @param  {object} op 実行オプション
   * @return {Bool}      実行結果 ok/ng true/false
   * @method
   * @override
   */
  open(op, mode) {
    let me=this, conn, rc; op=op||{};
    op.server=op.server||'localhost'; op.port=op.port||'3306';
    op.user=op.user||'mysql'; op.psw=op.psw||'mysql';
    me.DbName=op.db; me.Table=op.table;
    if(mode){
      conn={host: op.server, port: op.port, user: op.user, password: op.psw, database: op.db};
    }else{
      conn={host: op.server, port: op.port, user: op.user, password: op.psw};
    }
    me.db=Ms.createConnection(conn);
    let wid=me.ready(); me.error='';
    me.db.connect(function(err){
      if(err){
        me.error=err; rc=false;
        console.error('mysql(ERROR):' + err.stack);
      }else{
        console.log('mysql(OK):' + me.db.threadId);
      }
      me.post(wid);
    });
    me.wait();
    if(!mode){
      let ss={sql: 'CREATE DATABASE '+op.db};
      rc=this.sql(ss);
    }
    return rc;
  }
  /**ok
   * SQLの実行
   * @param  {Object} op  オプション{sql: SQL文}
   * @return {Bool}       OK/NG true/false
   * @method
   */
  sql(op) {
    let me=this, wid, rc;
    me.Save=op.sql; me.error='';
    wid=me.ready();
    me.db.query(op.sql, function (err, res, fields) {
      if(err){me.error=err; rc=false;}
      else{rc={}; rc.rows=res; rc.fields=fields;}
      me.post(wid, rc);
    });
    rc=me.wait();
    //op.res=rc; op.error=me.error;
    return rc;
  }
  query(op) { // 照会(SQL文)=>件数 this->rec結果配列[n][name]
    let me=this, rc, i, ss={sql: ''};
    if(typeof(op)=='string'){ss.sql=op;}else{ss.sql=op.sql;}
    rc=me.sql(ss); me.REC=[];
    if(rc){for(i in rc.rows){me.REC[i]=rc.rows[i];} rc=rc.rows.length;}
    if(typeof(op)=='object'){op.rec=me.REC; op.count=rc;}
    return rc;
  }
  escape(txt){
    return txt;
    //    return Ms.escape(txt);
  }
  create(op) {
    let sql='create table '+op.table+' ('; let tp;
    let c='', k; for(k in op.items){
      if(typeof(op.items[k])=='number'){tp='VARCHAR('+op.items[k]+')';}
      else{
        switch(op.items[k]){
        case 'num': tp='BIGINT'; break;
        case 'int': tp='INTEGER'; break;
        case 'text': tp='LONGTEXT'; break;
        case 'date': tp='DATETIME'; break;
        case 'binary': tp='LONGBLOG'; break;
        default: tp=op.items[k];
        }
      }
      sql+=c+k+' '+tp; c=', ';
    }
    sql+=', PRIMARY KEY(';
    c=''; for(let i in op.keys){sql+=c+op.keys[i]; c=', ';} sql+='));';
    return this.sql(sql);
  }
  index (table, name, keys, unique){
    if(unique){unique='UNIQUE ';}else{unique='';}
    let sql='ceate '+unique+'index '+name+' on '+table+' using BTREE (';
    let c=''; for(let i in keys){sql+=c+keys[i];} sql+=');';
    return this.sql(sql);
  }
};
