const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = process.env.PORT || 10000;
const rooms = new Map();
const publicDir = path.join(__dirname, 'public');

function id(){ return crypto.randomBytes(8).toString('hex'); }
function roomCode(){
  let c; do c = Math.random().toString(36).slice(2,7).toUpperCase(); while(rooms.has(c));
  return c;
}
function shuffle(a){ for(let i=a.length-1;i>0;i--){ const j=Math.floor(Math.random()*(i+1)); [a[i],a[j]]=[a[j],a[i]]; } return a; }
function makeRoom(host, name){
  const code=roomCode();
  const room={code, hostId:host, players:[], state:'lobby', deck:[], current:null, pot:0, turn:0, history:[], winner:null};
  room.players.push({id:host,name:name||'Người chơi 1',chips:11,cards:[],connected:true,bot:false});
  rooms.set(code,room); return room;
}
function score(cards,chips){
  const s=[...cards].sort((a,b)=>a-b); let total=0;
  for(let i=0;i<s.length;i++) if(i===0 || s[i]!==s[i-1]+1) total+=s[i];
  return total-chips;
}
function snapshot(room){
  return {
    code:room.code,state:room.state,hostId:room.hostId,turn:room.turn,current:room.current,pot:room.pot,winner:room.winner,
    deckCount:room.deck.length,history:room.history.slice(-12),
    players:room.players.map(p=>({id:p.id,name:p.name,chips:p.chips,cards:[...p.cards].sort((a,b)=>a-b),connected:p.connected,bot:p.bot,score:room.state==='finished'?score(p.cards,p.chips):undefined}))
  };
}
function broadcast(room){ const data=JSON.stringify({type:'state',state:snapshot(room)}); room.players.forEach(p=>p.ws&&p.ws.readyState===1&&p.ws.send(data)); }
function send(ws,type,payload){ if(ws&&ws.readyState===1) ws.send(JSON.stringify({type,...payload})); }
function getPlayer(room,pid){ return room.players.find(p=>p.id===pid); }
function begin(room){
  if(room.players.length<3) return {error:'Cần ít nhất 3 người để bắt đầu.'};
  room.state='playing'; room.winner=null; room.history=[]; room.pot=0; room.turn=0;
  const deck=[]; for(let n=3;n<=35;n++) deck.push(n); shuffle(deck); room.deck=deck.slice(9);
  room.players.forEach(p=>{p.chips=11;p.cards=[];}); room.current=room.deck.pop();
  broadcast(room); return {};
}
function take(room,pid){
  if(room.state!=='playing') return {error:'Ván chơi chưa diễn ra.'};
  const p=getPlayer(room,pid); if(!p || room.players[room.turn].id!==pid) return {error:'Chưa đến lượt bạn.'};
  if(room.current==null) return {error:'Không có lá bài.'};
  p.cards.push(room.current); p.chips += room.pot; room.history.push({card:room.current,player:p.name,pot:room.pot}); room.pot=0;
  room.current=room.deck.pop();
  if(room.current==null){ room.state='finished'; const scores=room.players.map(x=>({id:x.id,score:score(x.cards,x.chips)})).sort((a,b)=>a.score-b.score); room.winner=scores[0].id; }
  else room.turn=(room.turn+1)%room.players.length;
  broadcast(room); return {};
}
function pass(room,pid){
  if(room.state!=='playing') return {error:'Ván chơi chưa diễn ra.'};
  const p=getPlayer(room,pid); if(!p || room.players[room.turn].id!==pid) return {error:'Chưa đến lượt bạn.'};
  if(p.chips<=0) return {error:'Bạn không còn xu — bắt buộc phải lấy bài.'};
  p.chips--; room.pot++; room.history.push({card:room.current,player:p.name,action:'pass'}); room.turn=(room.turn+1)%room.players.length; broadcast(room); return {};
}
function removePlayer(room,pid){
  const idx=room.players.findIndex(p=>p.id===pid); if(idx<0) return;
  room.players.splice(idx,1); if(room.players.length===0){rooms.delete(room.code);return;}
  if(room.hostId===pid) room.hostId=room.players[0].id;
  if(room.turn>=room.players.length) room.turn=0;
  if(room.state==='playing' && room.players.length<3) room.state='lobby';
  broadcast(room);
}
function addBot(room){
  if(room.state!=='lobby' || room.players.length>=7) return {error:'Không thể thêm bot lúc này.'};
  const num=room.players.filter(p=>p.bot).length+1; room.players.push({id:'bot-'+id(),name:'Bot '+num,chips:11,cards:[],connected:true,bot:true}); broadcast(room); return {};
}
function botThink(room){
  if(room.state!=='playing') return;
  const p=room.players[room.turn]; if(!p||!p.bot) return;
  const n=room.current; const threshold=Math.max(2, Math.min(8, Math.floor(n/6)));
  const action=(p.chips===0 || Math.random()<0.28 || (room.pot>=threshold && Math.random()<0.55))?'take':'pass';
  action==='take'?take(room,p.id):pass(room,p.id);
}

const server=http.createServer((req,res)=>{
  let file=req.url==='/'?'/index.html':req.url.split('?')[0];
  const fp=path.normalize(path.join(publicDir,file));
  if(!fp.startsWith(publicDir)){res.writeHead(403);return res.end('Forbidden');}
  fs.readFile(fp,(err,data)=>{ if(err){res.writeHead(404);return res.end('Not found');} const ext=path.extname(fp); const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.svg':'image/svg+xml'}; res.writeHead(200,{'Content-Type':types[ext]||'application/octet-stream','Cache-Control':'no-store'});res.end(data);});
});
const wss=new WebSocket.Server({server});
wss.on('connection',(ws)=>{
  let room=null, pid=null;
  ws.on('message',(raw)=>{
    try{
      const m=JSON.parse(raw); const type=m.type;
      if(type==='create'){
        pid=id(); room=makeRoom(pid,m.name); const p=getPlayer(room,pid);p.ws=ws; send(ws,'joined',{room:room.code,playerId:pid}); broadcast(room); return;
      }
      if(type==='join'){
        const r=rooms.get(String(m.code||'').toUpperCase()); if(!r) return send(ws,'error',{message:'Không tìm thấy phòng.'});
        if(r.state!=='lobby') return send(ws,'error',{message:'Ván đã bắt đầu, không thể vào phòng.'}); if(r.players.length>=7) return send(ws,'error',{message:'Phòng đã đủ 7 người.'});
        pid=id(); room=r; const p={id:pid,name:(m.name||'Người chơi').slice(0,20),chips:11,cards:[],connected:true,bot:false,ws}; room.players.push(p); send(ws,'joined',{room:room.code,playerId:pid}); broadcast(room); return;
      }
      if(!room||!pid) return send(ws,'error',{message:'Bạn chưa vào phòng.'});
      if(type==='start') { if(room.hostId!==pid)return send(ws,'error',{message:'Chỉ chủ phòng mới được bắt đầu.'}); const e=begin(room); if(e.error)return send(ws,'error',e); return; }
      if(type==='addBot'){ if(room.hostId!==pid)return send(ws,'error',{message:'Chỉ chủ phòng mới thêm bot.'}); const e=addBot(room);if(e.error)send(ws,'error',e);return; }
      if(type==='take'){const e=take(room,pid);if(e.error)send(ws,'error',e);return;}
      if(type==='pass'){const e=pass(room,pid);if(e.error)send(ws,'error',e);return;}
      if(type==='restart'){if(room.hostId!==pid)return send(ws,'error',{message:'Chỉ chủ phòng mới được chơi lại.'}); if(room.state!=='finished')return; begin(room);return;}
      if(type==='kick'){if(room.hostId!==pid)return send(ws,'error',{message:'Chỉ chủ phòng mới được kick.'}); const target=getPlayer(room,m.playerId); if(!target||target.id===room.hostId)return; send(target.ws,'kicked',{}); if(target.ws)target.ws.close(); removePlayer(room,target.id);return;}
      if(type==='rename'){const p=getPlayer(room,pid);if(p){p.name=String(m.name||p.name).slice(0,20);broadcast(room);}return;}
    }catch(e){ send(ws,'error',{message:'Dữ liệu không hợp lệ.'}); }
  });
  ws.on('close',()=>{ if(room&&pid){const p=getPlayer(room,pid);if(p){p.connected=false;p.ws=null;broadcast(room);} }});
});
setInterval(()=>rooms.forEach(botThink),900);
server.listen(PORT,()=>console.log(`No Thanks online listening on ${PORT}`));
