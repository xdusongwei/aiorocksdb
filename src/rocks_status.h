class RocksStatus{
    public:
        RocksStatus(){
            this->code = 0U;
            this->subCode = 0U;
            this->severity = 0U;
        }

        void fromStatus(Status& s){
            this->code = s.code();
            this->subCode = s.subcode();
            this->severity = 0U;
        }

        bool isOk(){
            return code == Status::Code::kOk;
        }

        void setCode(unsigned char code){
            this->code = code;
        }

        void setSubCode(unsigned char subCode){
            this->subCode = subCode;
        }

        void setSeverity(unsigned char severity){
            this->severity = severity;
        }

        unsigned char getCode(){
            return code;
        }

        unsigned char getSubCode(){
            return subCode;
        }

        unsigned char getSeverity(){
            return severity;
        }

    private:
        unsigned char code;
        unsigned char subCode;
        unsigned char severity;
};
