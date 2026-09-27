import {test,expect} from '@playwright/test';
const run={run_id:'test',sequence:1,status:'completed',stage:'completed',entities:['wintermute'],from_date:'2024-01-01',to_date:'2025-01-01',progress:1};
const pattern={pattern_key:'p',entity_id:'wintermute',asset_key:'ethereum:0x'+'abcdef'.repeat(20),asset_label:'TOKEN',chain:'ethereum',pattern_label:'VERY_LONG_ACTION_'.repeat(30),intent_label:'buy',horizon_seconds:300,n:20,mean_return:.05,negative_rate:.2,positive_rate:.8,direction:'up',maturity:'PROMISING'};
test.beforeEach(async({page})=>{
 await page.route('**/api/**',async route=>{
  const u=new URL(route.request().url());let data;
  if(u.pathname.endsWith('/quality'))data={entities:[],errors:[],requests:[]};
  else if(u.pathname.endsWith('/patterns'))data={items:[pattern],total:1};
  else if(u.pathname.endsWith('/stream'))return route.fulfill({contentType:'text/event-stream',body:': open\n\n'});
  else data={run,totals:{wallets_total:1,wallets_processed:1},entities:[],patterns:[],feed:[],errors:[],queues:{episode_builder:0,llm_classification:0,market_labeling:0,analysis:0},speed:{api:{},eta:{collection:0,episode:0,llm:0,market:0,analysis:0,total:0}},database_size:1000};
  await route.fulfill({json:data});
 });
});
for(const width of [320,375,768,1440])test(`patterns fit ${width}px`,async({page})=>{
 await page.setViewportSize({width,height:900});await page.goto('/runs/test');
 await expect(page.getByRole('heading',{name:'Pattern Explorer'})).toBeVisible();
 await expect(page.locator('.patternCard')).toHaveCount(1);
 const overflow=await page.evaluate(()=>[...document.querySelectorAll('*')].filter(e=>e.getBoundingClientRect().right>innerWidth+1).map(e=>({tag:e.tagName,cls:e.className,width:e.getBoundingClientRect().width,right:e.getBoundingClientRect().right})).slice(0,15));
 expect(overflow).toEqual([]);
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 const box=await page.locator('.patternCard').boundingBox();expect(box.x+box.width).toBeLessThanOrEqual(width);
 await page.screenshot({path:`test-results/terminal-${width}.png`,fullPage:true});
});
test('reactive navigation and browser history',async({page})=>{
 await page.goto('/runs/test');await page.getByRole('button',{name:'Data quality'}).click();
 await expect(page.getByRole('heading',{name:'Data quality'})).toBeVisible();
 await page.goBack();await expect(page.getByRole('heading',{name:'Pattern Explorer'})).toBeVisible();
 await page.goForward();await expect(page.getByRole('heading',{name:'Data quality'})).toBeVisible();
});
test('sample filter and pagination parameters',async({page})=>{
 await page.goto('/runs/test');await expect(page.locator('.patternCard')).toHaveCount(1);
 const request=page.waitForRequest(r=>r.url().includes('max_n=2'));
 await page.getByLabel('Sample',{exact:true}).selectOption('low');
 const url=new URL((await request).url());expect(url.searchParams.get('min_n')).toBe('1');
 expect(url.searchParams.get('limit')).toBe('24');
});
