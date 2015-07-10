(ns conduit.protocol)

(defprotocol Conduit
  (identifier [this])
  (verbose? [this])
  (receiver [this])
  (parse [this message])
  (unhandled [this message]))
