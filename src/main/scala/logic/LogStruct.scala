package logic

class LogStruct(val remotehost: String,
            val rfC931: String,
            val authuser: String,
            val date: String,
            val request: String,
            val status: Int,
            val bytes: Int) {
}
