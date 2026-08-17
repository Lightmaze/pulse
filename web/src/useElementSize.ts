import { useEffect, useRef, useState, type RefObject } from "react";

export interface Size {
  w: number;
  h: number;
}

/**
 * Element size for canvas sizing, robust to a hidden tab.
 *
 * ResizeObserver callbacks ride the rendering steps, so a tab that is
 * backgrounded when the element mounts never gets its first delivery — the
 * canvas stays stuck at the 300x150 default forever. Timers keep running
 * there, so an initial measurement retried on a short timer chain closes the
 * gap; the observer then handles every later change for free.
 */
export function useElementSize(ref: RefObject<HTMLElement | null>): Size {
  const [size, setSize] = useState<Size>({ w: 0, h: 0 });
  const sizeRef = useRef(size);

  useEffect(() => {
    const el = ref.current;
    if (el === null) return;

    let timer = 0;
    const measure = () => {
      const next = { w: el.clientWidth, h: el.clientHeight };
      if (next.w === sizeRef.current.w && next.h === sizeRef.current.h) return;
      sizeRef.current = next;
      setSize(next);
    };

    const retryUntilMeasured = (attempt: number) => {
      measure();
      if (sizeRef.current.w > 0 && sizeRef.current.h > 0) return;
      if (attempt >= 12) return;
      timer = window.setTimeout(() => retryUntilMeasured(attempt + 1), 150);
    };
    retryUntilMeasured(0);

    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => {
      window.clearTimeout(timer);
      ro.disconnect();
    };
  }, [ref]);

  return size;
}
