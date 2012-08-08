package com.netflix.edda

object Record {
    def fromBean(obj: Any): Record = {
        new Record("TODO", "TODO")
    }
}

class Record(val id: String, val data: Any) {
    def copy(id: String = id, data: Any = data) = new Record(id,data)
}

