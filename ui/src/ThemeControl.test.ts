import test, { type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { act, createElement } from 'react';
import { JSDOM } from 'jsdom';
import { createViteTestServer } from './test-support';

async function renderThemeControl(t: TestContext) {
  const dom = new JSDOM(
    '<!doctype html><html><body><button>Outside</button><div id="root"></div></body></html>',
    { pretendToBeVisual: true, url: 'https://example.test/ui/' }
  );
  const globals = {
    window: dom.window,
    document: dom.window.document,
    Node: dom.window.Node,
    HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
  };
  const descriptors = Object.fromEntries(
    Object.keys(globals).map((name) => [name, Object.getOwnPropertyDescriptor(globalThis, name)])
  );
  for (const [name, value] of Object.entries(globals))
    Object.defineProperty(globalThis, name, { configurable: true, value });
  const server = await createViteTestServer(t);
  const { ThemeControl } = await server.ssrLoadModule('/src/ThemeControl.tsx');
  const { createRoot } = await import('react-dom/client');
  const document = dom.window.document;
  const root = createRoot(document.getElementById('root')!);
  await act(async () => root.render(createElement(ThemeControl)));
  t.after(async () => {
    await act(async () => root.unmount());
    for (const [name, descriptor] of Object.entries(descriptors)) {
      if (descriptor) Object.defineProperty(globalThis, name, descriptor);
      else Reflect.deleteProperty(globalThis, name);
    }
    dom.window.close();
  });
  const button = (label: string) => {
    const found = document.querySelector<HTMLButtonElement>(`button[aria-label="${label}"]`);
    assert.ok(found, `${label} must be rendered`);
    return found;
  };
  return {
    document,
    outside: document.querySelector('body > button') as HTMLButtonElement,
    trigger: () => document.querySelector('.theme-trigger') as HTMLButtonElement,
    button,
    menuOpen: () => document.querySelector('.theme-choices') !== null,
    resolved: () => document.documentElement.dataset.theme,
    stored: () => dom.window.localStorage.getItem('zeroshot.theme.v1'),
    /** Presses like a mouse. Chromium focuses a pressed button; Safari clears focus instead. */
    async press(target: HTMLElement, focusesButtons: boolean) {
      await act(async () => {
        target.dispatchEvent(new dom.window.PointerEvent('pointerdown', { bubbles: true }));
        if (focusesButtons) target.focus();
        else if (document.activeElement !== document.body)
          (document.activeElement as HTMLElement).blur();
      });
      await act(async () => target.click());
    },
  };
}

for (const focusesButtons of [true, false]) {
  const engine = focusesButtons ? 'focus' : 'do not focus';
  test(`theme choices apply in browsers that ${engine} pressed buttons`, async (t) => {
    const page = await renderThemeControl(t);
    assert.equal(page.resolved(), 'light');

    await page.press(page.trigger(), focusesButtons);
    assert.ok(page.menuOpen());
    await page.press(page.button('Dark theme'), focusesButtons);
    assert.equal(page.resolved(), 'dark');
    assert.equal(page.stored(), 'dark');
    assert.equal(page.trigger().getAttribute('aria-label'), 'Color theme: dark');
    assert.equal(page.menuOpen(), false);
    assert.equal(page.document.activeElement, page.trigger());

    await page.press(page.trigger(), focusesButtons);
    await page.press(page.button('Light theme'), focusesButtons);
    assert.equal(page.resolved(), 'light');
    assert.equal(page.stored(), 'light');
  });

  test(`the theme button closes its menu in browsers that ${engine} pressed buttons`, async (t) => {
    const page = await renderThemeControl(t);
    await page.press(page.trigger(), focusesButtons);
    assert.ok(page.menuOpen());
    await page.press(page.trigger(), focusesButtons);
    assert.equal(page.menuOpen(), false);
    assert.equal(page.stored(), null);
  });
}

test('the theme menu closes when focus moves elsewhere or a press lands outside', async (t) => {
  const page = await renderThemeControl(t);
  await page.press(page.trigger(), false);
  assert.equal(page.document.activeElement, page.button('System theme'));
  await act(async () => page.outside.focus());
  assert.equal(page.menuOpen(), false);

  for (const focusesButtons of [true, false]) {
    await page.press(page.trigger(), focusesButtons);
    assert.ok(page.menuOpen());
    await page.press(page.outside, focusesButtons);
    assert.equal(page.menuOpen(), false);
  }
  assert.equal(page.stored(), null);
});
