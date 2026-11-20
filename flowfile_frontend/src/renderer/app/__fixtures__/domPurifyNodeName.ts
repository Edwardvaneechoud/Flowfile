/**
 * happy-dom answers `""` from `Node.prototype.nodeName`'s own getter (each subclass overrides
 * it), and DOMPurify reads that getter, so under happy-dom it sees every element as unnamed and
 * sanitises nothing. Import this before DOMPurify: it makes the base getter answer what the
 * node's own class says, as a browser's does.
 */
const base = Object.getOwnPropertyDescriptor(Node.prototype, "nodeName");

Object.defineProperty(Node.prototype, "nodeName", {
  configurable: true,
  get(this: Node): string {
    let proto = Object.getPrototypeOf(this);
    while (proto && proto !== Node.prototype) {
      const own = Object.getOwnPropertyDescriptor(proto, "nodeName");
      if (own?.get) return own.get.call(this);
      proto = Object.getPrototypeOf(proto);
    }
    return base?.get?.call(this) ?? "";
  },
});
