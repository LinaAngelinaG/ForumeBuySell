CREATE TABLE Product(
    productName CHAR(20) PRIMARY KEY
);

CREATE TABLE Device(
    cookie INTEGER PRIMARY KEY
);

CREATE TABLE Person(
    personId SERIAL PRIMARY KEY,
    fullName CHAR(60),
    nickName CHAR(20) UNIQUE NOT NULL,
    email CHAR(20) UNIQUE NOT NULL,
    phoneNumber CHAR(20) UNIQUE NOT NULL
);

CREATE TABLE PersonOnDevice(
    cookie INTEGER REFERENCES Device(cookie),
    personId INTEGER REFERENCES Person(personId),
    PRIMARY KEY (cookie,personId)
);

CREATE TABLE Seller
(
    sellerId SERIAL PRIMARY KEY,
    sellerRate FLOAT DEFAULT(NULL),
    FOREIGN KEY (sellerId) REFERENCES Person (personId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE
);

CREATE TABLE SellersProduct
(
    productId SERIAL PRIMARY KEY,
    availableAmountOfProducts INTEGER NOT NULL ,
    priceOfProduct DECIMAL(10,2) NOT NULL,
    productName CHAR(20) NOT NULL,
    sellerId INTEGER NOT NULL,
    FOREIGN KEY (productName) REFERENCES Product (productName)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE,
    FOREIGN KEY (sellerId) REFERENCES Seller(sellerId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE
);

CREATE TABLE Buyer
(
    buyerId SERIAL PRIMARY KEY,
    adressOfDelivery VARCHAR(20) NOT NULL,
    buyerRate FLOAT DEFAULT(0),
    FOREIGN KEY (buyerId) REFERENCES Person (personId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE
);

CREATE TABLE LineInProductList
(
    productId INTEGER,
    listId SERIAL,
    PRIMARY KEY(productId,listId),
    numberOfItems INTEGER DEFAULT (1) NOT NULL ,
    FOREIGN KEY (productId) REFERENCES SellersProduct(productId)
                            ON DELETE SET NULL
                            ON UPDATE CASCADE,
    FOREIGN KEY (listId) REFERENCES Buyer(buyerId)
                            ON DELETE SET NULL
                            ON UPDATE CASCADE
);

CREATE TYPE stuffRole AS ENUM('moderator','admin');

CREATE TABLE Stuff(
    stuffId SERIAL PRIMARY KEY REFERENCES Person(personId),
    role stuffRole NOT NULL
);

CREATE TABLE CommunicationChannel(
  channelId SERIAL PRIMARY KEY,
  sellerId SERIAL NOT NULL,
  buyerId SERIAL NOT NULL,
  moderatorId SERIAL NOT NULL,
  FOREIGN KEY(sellerId) REFERENCES Seller(sellerId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE,
  FOREIGN KEY(buyerId) REFERENCES Buyer(buyerId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE,
  FOREIGN KEY(moderatorId) REFERENCES Stuff(stuffId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE
);

CREATE TABLE Message(
    channelId INTEGER REFERENCES CommunicationChannel(channelId),
    numberOfMessage INTEGER,
    PRIMARY KEY(channelId, numberOfMessage),
    userIdTo INTEGER,
    userIdFrom INTEGER,
    timeOfSending timestamp NOT NULL DEFAULT (current_timestamp),
    messageText VARCHAR(1000) NOT NULL
);

CREATE TYPE status as enum ('started', 'in process', 'finished');

CREATE TABLE disputeReason(
    codeNumber INTEGER PRIMARY KEY,
    codeName CHAR(20) NOT NULL UNIQUE
);

CREATE TABLE Dispute(
    channelId SERIAL PRIMARY KEY ,
    statusOfDispute status NOT NULL DEFAULT ('started'),
    disputeReasonCode INTEGER NOT NULL REFERENCES  disputeReason(codeNumber),
    endOfDispute TIMESTAMP NOT NULL,
    FOREIGN KEY (channelId) REFERENCES  CommunicationChannel(channelId)
                            ON DELETE SET NULL
                            ON UPDATE CASCADE
 );

CREATE TABLE DeliveryList(
    deliveryStatus status NOT NULL DEFAULT ('in process'),
    deliveryListId SERIAL PRIMARY KEY,
    buyerId INTEGER  REFERENCES Buyer(buyerId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE,
    deliveryDate TIMESTAMP NOT NULL,
    adressOfDelivery CHAR(50) NOT NULL,
    dateOfFinishedDelivery timestamp NOT NULL DEFAULT (now())
);

CREATE TABLE Delivery(
    delListId INTEGER REFERENCES DeliveryList(deliveryListId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE,
    deliveryId SERIAL REFERENCES SellersProduct(productId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE,
    PRIMARY KEY(deliveryId,delListId),
    numberOfProducts INTEGER DEFAULT (0) NOT NULL,
    priceOfProduct DECIMAL(10,2) NOT NULL
);

CREATE TYPE dealStatus as ENUM ('opened', 'denied','finished');

CREATE TABLE Currency(
    currencyId SERIAL PRIMARY KEY,
    currencyPrice FLOAT NOT NULL,
    currencyName CHAR(20) NOT NULL
);

CREATE TABLE Wallet(
    walletId SERIAL PRIMARY KEY,
    currencyCode INTEGER NOT NULL,
    FOREIGN KEY (currencyCode) REFERENCES Currency(currencyId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE
);

CREATE TABLE SmartContract(
    contractId SERIAL PRIMARY KEY,
    walletId INTEGER NOT NULL,
    FOREIGN KEY (walletId) REFERENCES Wallet(walletId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE
);
CREATE TABLE PaymentInvoice(
    billId SERIAL PRIMARY KEY,
    contract INTEGER NOT NULL ,
    summForPayment DECIMAL(10,2) NOT NULL,
    paymentStatus status NOT NULL DEFAULT('started'),
    FOREIGN KEY (contract) REFERENCES SmartContract(contractId)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
);

CREATE TABLE Deal(
    contractId INTEGER PRIMARY KEY,
    listId INTEGER NOT NULL,
    dealStatus dealStatus NOT NULL DEFAULT ('opened'),
    dealOpeningDate TIMESTAMP NOT NULL DEFAULT(current_timestamp),
    dealComment CHAR(100) DEFAULT('No comments'),
    FOREIGN KEY (contractId) REFERENCES SmartContract(contractId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE
);

CREATE TABLE BuyedProduct(
    listId SERIAL,
    buyerId INTEGER REFERENCES Buyer(buyerId)
                            ON DELETE RESTRICT
                            ON UPDATE CASCADE,
    PRIMARY KEY(listId,productId),
    productId INTEGER,
    numberOfProducts INTEGER NOT NULL,
    productPrice DECIMAL(10,2) NOT NULL,
    dateOfFinishedDelivery timestamp NOT NULL DEFAULT (now()),
    FOREIGN KEY (productId) REFERENCES  SellersProduct(productId)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE
);

CREATE TABLE HistoricalCurrencyExchange(
  currencyFrom INTEGER REFERENCES Currency(currencyId),
  currencyTo INTEGER REFERENCES Currency(currencyId),
  value double precision NOT NULL,
  dateAndTime timestamp DEFAULT now(),
  PRIMARY KEY (currencyFrom,currencyTo,dateAndTime)
);

CREATE OR REPLACE FUNCTION updateDelivery() RETURNS TRIGGER AS $dealIsFinishing$
    DECLARE
            iter LineInProductList;
            save integer;
            delid integer;
            adress varchar(50);
            userId integer;
            price decimal;
    BEGIN
        IF (NEW.paymentStatus = 'finished') THEN

            UPDATE Deal SET dealStatus = 'finished'
                WHERE(contractId = new.contract);

            SELECT listId
            INTO save
            FROM Deal
            WHERE(contractId = new.contract);

            userId = save;

            SELECT adressOfDelivery
            INTO adress
            FROM Buyer
            WHERE (buyerId = userId);

            INSERT INTO DeliveryList VALUES (default,default,save,now()+interval '14 days',adress,default)
            RETURNING  deliveryListId INTO delid;

            UPDATE Deal SET listid = delid
                WHERE(contractId = new.contract);

            FOR iter IN
                SELECT * FROM LineInProductList WHERE (listId = save)
            LOOP
                SELECT priceOfProduct INTO price FROM SellersProduct
                    WHERE (productId = iter.productId);
                INSERT INTO Delivery VALUES (delid,iter.productId,iter.numberOfItems,price);
            END LOOP;

            DELETE FROM LineInProductList WHERE listId = save;
        END IF;
        RETURN NEW;
    END;
$dealIsFinishing$ LANGUAGE plpgsql;

CREATE TRIGGER dealIsFinishing
BEFORE INSERT OR UPDATE ON PaymentInvoice
    FOR EACH ROW EXECUTE FUNCTION updateDelivery();

CREATE OR REPLACE FUNCTION updateBuyed() RETURNS TRIGGER AS $deliveryIsCompleted$
    DECLARE
            iter LineInProductList;
            num integer;
            save integer;
            price decimal;
            listid integer;
    BEGIN
        IF (NEW.deliveryStatus = 'finished') THEN
            save = OLD.deliveryListId;
            SELECT deliveryListId INTO listid FROM DeliveryList
                                              WHERE (buyerId = save);
            FOR iter IN
                SELECT * FROM Delivery WHERE (delListId = listid)
            LOOP
                SELECT priceOfProduct INTO price FROM SellersProduct
                    WHERE (productId = iter.productId);
                SELECT numberOfItems INTO num FROM LineInProductList
                    WHERE (listId = iter.listId AND productId = iter.productId);
                INSERT INTO BuyedProduct VALUES (default,save,iter.productId,iter.numberOfItems,price,default);
            END LOOP;
                    END IF;

        DELETE FROM Delivery WHERE delListId =listid;
        DELETE FROM DeliveryList WHERE deliveryListId = listid;
        RETURN NEW;
    END;
$deliveryIsCompleted$ LANGUAGE plpgsql;

CREATE TRIGGER deliveryIsCompleted
BEFORE INSERT OR UPDATE ON DeliveryList
    FOR EACH ROW EXECUTE FUNCTION updateBuyed();

CREATE OR REPLACE FUNCTION checkSellerAndBuyer() RETURNS TRIGGER AS $checkBuyerIsNotSeller$
    DECLARE
        selid integer;
        buyid integer;
    BEGIN

        buyid = NEW.listId;

        SELECT sellerId INTO selid
                       FROM SellersProduct
                       WHERE productId = NEW.productid;

        IF(selid = NEW.listId) THEN
            RAISE EXCEPTION 'impossible to buy your own products';
        end if;

        RETURN NEW;
    END;
$checkBuyerIsNotSeller$ LANGUAGE plpgsql;

CREATE TRIGGER checkBuyerIsNotSeller
BEFORE INSERT OR UPDATE ON LineInProductList
    FOR EACH ROW EXECUTE FUNCTION checkSellerAndBuyer();

CREATE OR REPLACE FUNCTION checkParts() RETURNS TRIGGER AS $checkPartsInChannel$
    BEGIN
        IF(NEW.buyerid = NEW.sellerid) THEN
            RAISE EXCEPTION 'impossible to connect with yourself';
        end if;

         IF NOT (SELECT EXISTS (SELECT sellerId FROM Seller WHERE NEW.sellerid  = sellerId)) THEN
            RAISE EXCEPTION 'no such seller ( % ) to connect', NEW.sellerid;
        end if;

         IF NOT(SELECT EXISTS (SELECT buyerId FROM Buyer WHERE NEW.buyerid  = buyerId)) THEN
            RAISE EXCEPTION 'no such buyer ( % ) to connect', NEW.buyerid;
        end if;

        RETURN NEW;
    END;
$checkPartsInChannel$ LANGUAGE plpgsql;

CREATE TRIGGER checkPartsInChannel
BEFORE INSERT OR UPDATE ON CommunicationChannel
    FOR EACH ROW EXECUTE FUNCTION checkParts();

CREATE OR REPLACE FUNCTION checkPartsOfMes() RETURNS TRIGGER AS $checkPartsOfMessage$
    DECLARE
        buyerId integer;
        sellerId integer;
    BEGIN
        IF(NEW.userIdFrom = NEW.userIdTo) THEN
            ROLLBACK;
            RAISE EXCEPTION 'impossible to send message to yourself';
        end if;

        SELECT buyerid, sellerId INTO buyerId, sellerId FROM CommunicationChannel WHERE channelId = NEW.channelId;

        IF(buyerId <> NEW.userIdFrom
               AND
           buyerId <> NEW.userIdTo
               OR
           sellerId <> NEW.userIdFrom
               AND
           sellerId <> NEW.userIdTo) THEN
            ROLLBACK;
            RAISE EXCEPTION 'no match (userIdFrom, userIdTo) with (sellerId, buyerId) from CommunicationChannel';
        end if;
    END;
$checkPartsOfMessage$ LANGUAGE plpgsql;

CREATE TRIGGER checkPartsOfMessage
BEFORE INSERT OR UPDATE ON Message
    FOR EACH ROW EXECUTE FUNCTION checkPartsOfMes();

CREATE PROCEDURE get_deals_for_period_with_status(beginDate timestamp, endDate timestamp, status dealStatus)
    LANGUAGE plpgsql
    AS $$
    DECLARE
        contract integer;
        nick CHAR(20);
        date timestamp;
        iter lineinproductlist;
        list integer;
        sum DECIMAL(10,6);
        stat status;
        cursDeals CURSOR FOR
                            SELECT (SELECT * FROM Deal WHERE dealStatus = status)
                            FROM Deal
                            WHERE (dealOpeningDate <= endDate AND dealOpeningDate >= beginDate);

        BEGIN
            create table result (
                buyerNick CHAR(20),
                dateOfDeakOpening timestamp,
                totalSum DECIMAL(10,6),
                statusOfDeal status
            );
            /* проверить как временные переменные создавать*/
            OPEN cursDeals;
            LOOP
                FETCH cursDeals INTO contract,list,date,stat;
                IF NOT FOUND
                        THEN EXIT;
                        END IF;

                FOR iter IN
                            SELECT * FROM LineInProductList WHERE listId = list
                LOOP
                    nick = iter.listId;
                    END LOOP;

                INSERT INTO result VALUES (nick,date,sum,stat);
            END LOOP;

            CLOSE cursDeals;
            SELECT * FROM result;
        END
    $$;

CREATE FUNCTION get_deals_for_period(beginDate timestamp without time zone, endDate timestamp without time zone) RETURNS refcursor
    LANGUAGE plpgsql
    AS $$
    DECLARE
        ref refcursor;
        contract integer;
        nick CHAR(20);
        date timestamp;
        iter LineInProductList;
        list integer;
        sum DECIMAL(10,6);
        stat dealStatus;
        cursDeals CURSOR FOR
                            SELECT * FROM Deal
                            WHERE (dealOpeningDate <= endDate AND dealOpeningDate >= beginDate);

        BEGIN

            OPEN cursDeals;
            LOOP
                FETCH cursDeals INTO contract,list,stat,date;
                IF NOT FOUND
                        THEN EXIT;
                        END IF;

                FOR iter IN
                            SELECT * FROM LineInProductList WHERE listId = list
                LOOP
                    nick = iter.listId;
                    END LOOP;
                with  result (
                buyerNick char(20),
                dateOfDealOpening timestamp,
                totalSum DECIMAL(10,6),
                statusOfDeal dealStatus
            )

                INSERT INTO result VALUES (nick,date,sum,stat);
            END LOOP;

            CLOSE cursDeals;

            OPEN ref FOR SELECT * FROM result;
            return ref;
        END
    $$;

CREATE OR REPLACE FUNCTION get_number_of(name CHAR(10)) RETURNS int
    LANGUAGE plpgsql
    AS $$
        BEGIN
            CASE name
                WHEN 'sellers'
                    THEN
                        RETURN (SELECT count(DISTINCT sellerId) FROM Seller);
                WHEN 'buyers'
                    THEN
                        RETURN (SELECT count(DISTINCT buyerId) FROM Buyer);
                ELSE
                    RAISE EXCEPTION 'impossible to count it: select "buyers" or "sellers"';
            END CASE;
        END
    $$;

CREATE OR REPLACE PROCEDURE create_deal_from_productlist(bId integer, walletId integer, comment VARCHAR DEFAULT null)
    LANGUAGE plpgsql
    AS $$

    DECLARE
        scId integer;
        paySum numeric;

    BEGIN

        INSERT INTO SmartContract VALUES (default,walletId)
                                  RETURNING contractId INTO scId;

        INSERT INTO Deal VALUES (scId,bId,default,default,comment);

        SELECT SUM(priceOfProduct)
        INTO paySum
        FROM (SELECT priceOfProduct
              FROM SellersProduct
              WHERE productId IN
                    (SELECT productId
                     FROM LineInProductList
                     WHERE listId = bId)) AS AllPrices;

        INSERT INTO PaymentInvoice VALUES (default,scId,paySum,default);
    END
    $$;


CREATE FUNCTION get_() RETURNS TABLE(id integer,adress varchar(20),rate double precision)
    LANGUAGE plpgsql
    AS $$
        BEGIN
            RETURN QUERY SELECT * FROM Buyer;
        END
    $$;

call update_exchange_for_currency();

CREATE OR REPLACE PROCEDURE update_exchange_for_currency()
    AS $$
    DECLARE
        currency_cursor refcursor;
        is_exist bool;
        currency_info record;
    BEGIN
        OPEN currency_cursor SCROLL FOR SELECT Currency.currencyId as cur_from,
                                               Currency.currencyPrice as price_from,
                                               c.currencyId as cur_to,
                                               c.currencyPrice as price_to
                                        FROM Currency CROSS JOIN Currency c
                                        WHERE  Currency.currencyId != c.currencyId;
        LOOP
            FETCH currency_cursor INTO currency_info;
            EXIT WHEN NOT FOUND;

            is_exist = (SELECT EXISTS(SELECT dateAndTime
                                      FROM HistoricalCurrencyExchange
                                      WHERE currencyFrom = currency_info.cur_from
                                        AND currencyTo = currency_info.cur_to
                                        AND now() - dateAndTime < make_interval(hours := 1))
                        );
            IF(is_exist) THEN
                CONTINUE;
            END IF;

            INSERT INTO HistoricalCurrencyExchange VALUES
                (currency_info.cur_from,
                 currency_info.cur_to,
                 currency_info.price_from/currency_info.price_to,
                 default);
        END LOOP;
    END
$$
LANGUAGE plpgsql;

call update_exchange_for_currency();

CREATE TRIGGER update_currency_exchange
AFTER INSERT OR UPDATE ON Currency
FOR EACH STATEMENT EXECUTE PROCEDURE update_exchange_for_currency();