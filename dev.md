# Notes

  * Result of a data query is a subobject of the DSGraph inside of the Fabric.
  * `decide_source` should somehow be renamed to indicate its fetching the source
  * dismbiguate `reflect!`, `recatalog` and `reload` and if necessary rename to enforce consistency
    * `reload` is how we reload (reconnect) the DB connection: at backend interface level
    * `reconnect!`: updates the DB catalog in the backend connection
    * `reflect!`: updates catalog in the fabric
  * need to figure out when we open/close connections to a DB.
  * get rid of `render` at Fabric.jl level
