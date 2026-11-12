'use strict';
(() => {
  const $ = id => document.getElementById(id);
  let session = null, token = '', timer = null, generation = 0, qrBlob = null, version = '';
  const stateNames = {creating:'正在创建二维码',waiting:'请用手机 QQ 扫码确认',ready:'扫码完成，核对应用后确认保存',validating:'正在验证凭证',saved:'已保存为禁用状态；请配置统一权限策略与 Webhook，再明确启用并重启',cancelled:'本地会话已取消',expired:'官方二维码已过期',timed_out:'本地轮询已超时',failed:'绑定失败，请重新创建'};
  async function api(path, options = {}) {
    const response = await fetch(path, {...options, cache:'no-store', headers:{'Authorization':'Bearer ' + token,'Content-Type':'application/json',...(options.headers || {})}});
    if (!response.ok) { const data = await response.json().catch(() => ({})); throw new Error(data.detail || '请求失败'); }
    return response;
  }
  function clearQR() { if (qrBlob) URL.revokeObjectURL(qrBlob); qrBlob = null; $('qq-qr').hidden = true; $('qq-connect').hidden = true; }
  function render(data) {
    session = data;
    $('qq-state').textContent = (stateNames[data.state] || data.state) + (data.app_id ? ' · AppID: ' + data.app_id : '');
    $('qq-apply').hidden = data.state !== 'ready';
    $('qq-cancel').hidden = !['creating','waiting','ready','validating'].includes(data.state);
    $('qq-start').disabled = !['saved','cancelled','expired','timed_out','failed'].includes(data.state);
    if (data.state !== 'waiting') clearQR();
    if (data.state === 'waiting') { $('qq-connect').href = data.connect_url; $('qq-connect').hidden = false; }
  }
  async function poll(epoch) {
    if (epoch !== generation || !session) return;
    try {
      const data = await (await api('/api/v1/qq/onboarding/sessions/' + session.session_id)).json();
      if (epoch !== generation) return;
      render(data);
      if (data.state === 'waiting' && !qrBlob) {
        const blob = await (await api('/api/v1/qq/onboarding/sessions/' + data.session_id + '/qr')).blob();
        if (epoch !== generation) return;
        qrBlob = URL.createObjectURL(blob); $('qq-qr').src = qrBlob; $('qq-qr').hidden = false;
      }
      if (['creating','waiting'].includes(data.state)) timer = setTimeout(() => poll(epoch), 2000);
    } catch (error) { if (epoch === generation) $('qq-state').textContent = error.message + '。可取消或刷新后重新开始。'; }
  }
  $('qq-bind-form').addEventListener('submit', async event => {
    event.preventDefault(); clearTimeout(timer); clearQR(); const epoch = ++generation;
    token = $('qq-token').value; $('qq-token').value = ''; $('qq-start').disabled = true;
    try {
      const config = await (await api('/api/v1/config')).json();
      version = config.saved_version || config.version;
      const data = await (await api('/api/v1/qq/onboarding/sessions', {method:'POST',body:JSON.stringify({instance_id:$('qq-instance').value,replace_app_id:$('qq-replace-app').value.trim() || null})})).json();
      if (epoch !== generation) return;
      render(data); await poll(epoch);
    } catch(error) { if(epoch === generation) { $('qq-state').textContent = error.message; $('qq-start').disabled = false; } }
  });
  $('qq-cancel').addEventListener('click', async () => {
    clearTimeout(timer); const epoch = ++generation; $('qq-cancel').disabled = true;
    try { const data = await (await api('/api/v1/qq/onboarding/sessions/' + session.session_id, {method:'DELETE'})).json(); if(epoch === generation) render(data); }
    catch(error) { $('qq-state').textContent = error.message; }
    finally { $('qq-cancel').disabled = false; }
  });
  $('qq-apply').addEventListener('click', async () => {
    $('qq-apply').disabled = true; const epoch = generation;
    try {
      const config = await (await api('/api/v1/config')).json();
      version = config.saved_version || config.version;
      const data = await (await api('/api/v1/qq/onboarding/sessions/' + session.session_id + '/apply', {method:'POST',body:JSON.stringify({expected_version:version,expected_app_id:session.app_id})})).json();
      if(epoch === generation) { render(data); token = ''; }
    } catch(error) { if(epoch === generation) $('qq-state').textContent = error.message; }
    finally { $('qq-apply').disabled = false; }
  });
  window.addEventListener('pagehide', () => { generation++; clearTimeout(timer); clearQR(); token = ''; });
})();
