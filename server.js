// Couples Ludo server: static page + tiny realtime document store over WebSocket
const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PW=process.env.ADMIN_PASS||'admin123',PORT=process.env.PORT||3000;
const docs=new Map(),touched=new Map(),subs=new Map();
const srv=http.createServer((q,r)=>{if(q.url=='/health')return r.end('ok');
 r.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});r.end(fs.readFileSync(path.join(__dirname,'public','index.html')))});
const wss=new WebSocketServer({server:srv,maxPayload:20000});
const okp=p=>typeof p=='string'&&/^rooms\/[A-Z0-9]{6}(\/msgs(\/[\w-]+)?)?$/.test(p);
const depth=p=>p.split('/').length;
const colDocs=p=>[...docs].filter(([k])=>k.startsWith(p+'/')&&depth(k)==depth(p)+1).map(([k,d])=>({id:k.split('/').pop(),d})).sort((a,b)=>(a.d.t||0)-(b.d.t||0)).slice(-100);
const push=(w,k,s)=>{if(w.readyState==1)w.send(JSON.stringify(s.kind=='doc'?{k,data:docs.get(s.p)||null}:{k,docs:colDocs(s.p)}))};
const notify=p=>subs.forEach((ss,w)=>Object.entries(ss).forEach(([k,s])=>{if((s.kind=='doc'&&s.p==p)||(s.kind=='col'&&p.startsWith(s.p+'/')))push(w,k,s)}));
const put=(p,d)=>{docs.set(p,d);touched.set(p.split('/')[1],Date.now());notify(p)};
wss.on('connection',w=>{subs.set(w,{});
 w.on('close',()=>subs.delete(w));
 w.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}
  const re=o=>w.readyState==1&&w.send(JSON.stringify({rid:m.rid,...o}));
  try{
   if(m.op=='auth')return m.pw===PW?(w.admin=true,re({})):re({err:'galat password'});
   if(m.op=='unsub'){delete subs.get(w)[m.k];return}
   if(m.op=='list'){if(!w.admin||m.p!='rooms')return re({err:'admin chahiye'});
    return re({docs:[...docs].filter(([k])=>depth(k)==2).map(([k,d])=>({id:k.split('/')[1],d}))})}
   if(!okp(m.p))return re({err:'bad path'});
   if(m.data&&JSON.stringify(m.data).length>6000)return re({err:'data bohot bada'});
   if(m.op=='get')return re({data:docs.get(m.p)||null});
   if(m.op=='sub'){subs.get(w)[m.k]={p:m.p,kind:m.kind=='col'?'col':'doc'};return push(w,m.k,subs.get(w)[m.k])}
   if(m.op=='set'&&depth(m.p)==2){if(docs.size>4000)return re({err:'server full'});put(m.p,m.data);return re({})}
   if(m.op=='upd'&&depth(m.p)==2){if(!docs.has(m.p))return re({err:'room nahi mila'});put(m.p,{...docs.get(m.p),...m.data});return re({})}
   if(m.op=='add'&&/\/msgs$/.test(m.p)&&docs.has(m.p.replace(/\/msgs$/,''))){put(m.p+'/'+Date.now().toString(36)+Math.random().toString(36).slice(2,6),m.data);return re({})}
   if(m.op=='del'&&depth(m.p)==2){const ks=[...docs.keys()].filter(k=>k==m.p||k.startsWith(m.p+'/'));ks.forEach(k=>docs.delete(k));ks.forEach(notify);return re({})}
   re({err:'bad op'})}catch(e){re({err:'server error'})}})});
setInterval(()=>{const now=Date.now();touched.forEach((t,c)=>{if(now-t>864e5){[...docs.keys()].filter(k=>k.split('/')[1]==c).forEach(k=>docs.delete(k));touched.delete(c)}})},36e5);
srv.listen(PORT,()=>console.log('Couples Ludo chal raha hai: port '+PORT));
