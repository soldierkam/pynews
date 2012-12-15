
$jit.TM.Squarified.implement({
    'setColor': function(json) {
      return json.data.$color;
    }
});

(function(){

    var state = {
        loaded: false,
        i: 0,
        tm: undefined,
        data: undefined,
        resizeTimerId: undefined,
        autorefresh: false,
        autoRefreshTimer: undefined,
        screenName: undefined
};
    var id = 0;
    var validCategories = {
        'SPORT': "s",
        'POLITICS': "p",
        'SCIENCE': "snc",
        'TECHNOLOGY': "tc",
        'BUSINESS': "b",
        'HEALTH': "m",
        'WORLD': "w",
        'NATION': "n",
        ELECTIONS: "el",
        HEADLINES: "h",
        SPOTLIGHT: "ir",
        'ENTERTAINMENT': "e",
        'ALL': "all"
    };
    var type2color={
        "w": "#777777",
        "b": "#77DD77",
        "n": "#7777DD",
        "tc": "#77DDDD",
        "snc": "#DD7777",
        "el": "#DDDD77",
        "p": "#DD77DD",
        "e": "#DDDDDD",
        "s": "#228855",
        "m": "#225588",
        "h": "#882255",
        "ir": "#558822"
    }

    var log = typeof window.console !== 'undefined' && typeof window.console.log === 'function' ? console.log.bind(console) : $.empty;

    function buildTm(){
        $('#infovis').css('display', '');
        state.tm = new $jit.TM.Squarified({
            //Where to inject the treemap.
            injectInto: 'infovis',

            //Add click handlers for
            //zooming the Treemap in and out
            addLeftClickHandler: false,
            addRightClickHandler: false,

            //When hovering a node highlight the nodes
            //between the root node and the hovered node. This
            //is done by adding the 'in-path' CSS class to each node.
            selectPathOnHover: false,

            Events: {
                enable: true,
                onMouseEnter: function(node, eventInfo) {
                    if(node) {
                        //add node selected styles and replot node
                        node.setCanvasStyle('shadowBlur', 7);
                        node.setData('color', '#888');
                        tm.fx.plotNode(node, tm.canvas);
                        tm.labels.plotLabel(tm.canvas, node);
                    }
                },
                onClick: function(n, o, e) {
                    var node = o.getNode();
                    if(node.data && node.data.href){
                        window.open(node.data.href)
                    } else {
                        log("Missing href");
                    }
                },
                onRightClick: function(n, o, e) {
                    var node = o.getNode();
                    log(node.data);
                }
            },

            Color: {
                //Allow coloring
                enable: true,
                //Set min value and max value constraints
                //for the *$color* property value.
                //Default's to -100 and 100.
                minValue: 0,
                maxValue: 256*256*256,
                //Set color range. Default's to reddish and greenish.
                //It takes an array of three
                //integers as R, G and B values.
                minColorValue: [0, 0, 0],
                maxColorValue: [255, 255, 255]
            },

            //Allow tips
            Tips: {
                enable: true,
                //add positioning offsets
                offsetX: 20,
                offsetY: 20,

                //implement the onShow method to
                //add content to the tooltip when a node
                //is hovered
                onShow: function(tip, node, isLeaf, domElement) {

                    tip.innerHTML = "<div class=\"tip-text\">" + this.makeHTMLFromData(node) + "</div>";
                },

                //Build the tooltip inner html by taking each node data property
                makeHTMLFromData: function(node){
                    var html = '', data = node.data;
                    if ("lead" in data){
                        html += data.lead;
                    }
                    return html;
                }
            },

            //Add the name of the node in the correponding label
            //This method is called once, on label creation.
            onCreateLabel: function(domElement, node){
                var style = domElement.style
                elem = $(domElement);

                domElement.innerHTML = node.name;
                style.display = '';
                //style.border = '1px solid transparent';
                if(node.data.leaf){
                    elem.addClass("leaf").addClass("noise");
                }
                //domElement.onmouseover = function() {
                //    style.border = '1px solid #9FD4FF';
                //};
                //domElement.onmouseout = function() {
                //    style.border = '1px solid transparent';
                //};
            },

            onAfterCompute: function () {
                hideLoader();
                state.loaded = true;
            },

            //Remove all element events before destroying it.
            onDestroyElement: function(content, tree, isLeaf, leaf){
                if(leaf.clearAttributes) leaf.clearAttributes();
            }
        });
    }

    function removeTm(){
        $('#infovis > div').remove();
        $('#infovis').css('display', 'none');
        state.tm = undefined;
    }

    function showLoader(){
        log("Show loader")
        //$('#infovis').css('display', 'none');
        $('#floatingBarsG').css('display', '');
    }

    function hideLoader(){
        log("Hide loader")
        //$('#infovis').css('display', '');
        $('#floatingBarsG').css('display', 'none');
    }

    function resizeBox( i, box ) {

        var width = $( box ).width(),
            textWidth,
            height = $( box ).height(),
            textHeight,
            minFontSize = 1,
            line = $( box ).wrapInner( '<span>' ).children()[ 0 ],
            n = 100;

        if (width < 25 || height < 25){
            n = 5
        }else if (width < 15 || height < 15){
            $( box ).css( 'font-size', 2 );
            return
        }

        $( box ).css( 'font-size', n );

        textWidth = $( line ).width();
        textHeight = $( line ).height();
        while ( textWidth > width || textHeight > height && n >= minFontSize) {
            $( box ).css( 'font-size', --n );
            textWidth = $( line ).width();
            textHeight = $( line ).height();
        }

        $( box ).text( $( line ).text() );

    };

    function refreshTreeMap(){
        buildTm();
        state.tm.loadJSON(state.data);
        state.tm.refresh();
        $( '.leaf' ).each(resizeBox);
    }

    function showTreeMap(data, user){
        state.data = data;
        state.user = user;
        refreshTreeMap();
    };

    function getValidCat(){
        var hash = window.location.hash.substring(1);
        var cat = 'ALL';
        if(hash in validCategories){
            cat = hash;
        }else{
            window.location.hash = cat;
            return;
        }
        return cat;
    };

    function findColor(cat){
        return type2color[cat] || "#DDDDDD";
    };

    function createLead(node){
        if(!node.text){
            return node.title;
        }
        var t = '<h3>' + node.title + '</h3><div>' + node.text.substr(0, 500);
        if(t.length == 500){
            t += "...</div>";
        }else{
            t += '</div>';
        }
        return t;
    };

    function getArea(node){
        //return node.mark
        var tweets = node.tweets;
        var mark = 0;
        $(tweets).each(function(i, tweet){
            var r;
            if (tweet.retweets){
                r = tweet.retweets * 0.5
            }else{
                r = 0.1
            }
            mark += r * 100 * tweet.user.followers
        })
        return mark;
    };


    function findLabel(cat){
        var o = {};
        $.each(validCategories, function(key, value) {
            o[value] = key;
        });
        return o[cat];
    }

    function cFactor(userCats, c) {
        var min, max, res;
        var labels = [];
        $.each(validCategories, function(key, value) {
            labels.push(value);
        });
        $(userCats).each(function(i, child){
            if(jQuery.inArray( child[0], labels) == -1){
                return;
            }

            if(min){
                min = Math.min(min, child[1]);
            }else{
                min = child[1];
            }
            if(max){
                max = Math.max(max, child[1]);
            }else{
                max = child[1];
            }
        });
        $(userCats).each(function(i, child){
            if(child[0] == c){
                res = child[1];
            }
        });
        res = res || min;
        return res / max;
    };

    function mFactor(userCats, c) {
        var sum = 0, c = 0;
        var labels = [];
        $.each(validCategories, function(key, value) {
            labels.push(value);
        });
        $(userCats).each(function(i, child){
            if(jQuery.inArray( child[0], labels) == -1){
                return;
            }
            c += 1;
            sum += child[1];
        });
        return c / sum;
    };

    function userCtxFactor(user, c){
        if(!user || !user.cat){
            return 1;
        }
        var uc = user.cat;
        var catFactor = cFactor(uc, c),
            meanFactor = userCtxMean(user, c),
            result = Math.pow(catFactor/meanFactor, 2);

        //log(cat + "=" + result);
        return result
    };

    function userCtxMean(user, c){
        var uc = user.cat;
        return mFactor(uc, c);
    };


    function buildUrlNode(urlData){
        state.i += 1;
        return {
            children: [],
            data:{
                "$area": getArea(urlData),
                "$color": findColor(urlData.cat),
                "href": urlData.url,
                lead: createLead(urlData),
                leaf: true,
                c: urlData.cat,
                l: urlData.len,
                "tweets": urlData.tweets
            },
            id: "id-" + state.i,
            name: urlData.title
        };
    }

    function buildNode(cat){
        state.i += 1;
        var name = findLabel(cat);
        return {
            children: [],
            data:{
                "$area": 0,
                "$color": findColor(cat),
                "href": undefined,
                lead: name,
                leaf: false,
                c: cat,
                l: undefined,
                "tweets": undefined
            },
            id: "id-" + state.i,
            name: name
        };
    }

    function prepareData(urlList, user){
        var cat2Nodes = {},
            cat2Root = {},
            cats=[];

        $(urlList).each(function (i, urlData) {
            var node = buildUrlNode(urlData),
                cat = urlData.cat;
            if(!(cat in cat2Nodes)){
                cat2Nodes[cat] = [];
                cats.push(cat);
            }
            cat2Nodes[cat].push(node);
        });

        $(cats).each(function(i, cat){
            var catNode = buildNode(cat);
            catNode.children = cat2Nodes[cat];
            cat2Root[cat] = catNode;
        });

        var root;
        if(cats.length == 1){
            root = cat2Root[cats[0]];
        }else if (cats.length > 1){
            root = buildNode("all");
            $(cats).each(function(i, cat){
                root.children.push(cat2Root[cat]);
            });
        }else{
            root = buildNode(validCategories[getValidCat()]);
        }
        calcArea(root, user);
        dropSmallArea(root, root);
        return root;
    }

    function sumChildren(ch, user){
        var sum = 0;
        $(ch).each(function(i, c){
            var factor = userCtxFactor(user, c.data.c);
            c.data["$area"] *= factor;
            c.data["$area"] += sumChildren(c.children, user);
            sum += c.data["$area"];
        });
        return sum;
    }

    function calcArea(node, user){
        node.data["$area"] += sumChildren(node.children, user);
    }

    function dropSmallArea(root, node){
        var toDelete = []
        var area = 0;
        $(node.children).each(function(i, child){
            var parentArea = root.data["$area"];
            var thisArea = child.data["$area"];
            var ratio = thisArea / parentArea;
            if( ratio < 0.0005){
                log("Ignore " + child.data.href + ": " + ratio)
                log(child.data.tweets)
                toDelete = toDelete.concat(child)
            }else{
                dropSmallArea(root, child)
            }
        });
        $(toDelete).each(function(i, itemtoRemove){
            node.children.splice($.inArray(itemtoRemove, node.children),1);
        });
    }

    function sendAjax(cat){
        state.loaded = false;
        state.data = undefined;
        log("Load " + cat + " for user " + state.screenName);
        $.ajax({
          url: '/newsmap.json',
          data: {
            c: cat,
            screenName: state.screenName
          },
          success: function( json ) {
              var treemapData = prepareData(json.data, json.user)
              if(json.user){
                updateTooltip(json.user.proc, json.user.phase);
              }
              showTreeMap(treemapData);
          }
        });
    }

    function handleWindowResize () {
        if(state.loaded){
            if(state.resizeTimerId){
                clearTimeout(state.resizeTimerId)
            }else{
                removeTm();
                showLoader();
                getValidCat();
            }
            log("Window resize")
            state.resizeTimerId = setTimeout(function (){
                log("Resizing...")
                state.resizeTimerId = undefined;
                refreshTreeMap();
            }, 500)
        }else{
            log("Ignore resize: not loaded")
        }
    };

    function triggerHashChange(){
        $(window).hashchange();
    }

    function onReady() {
        $('.dropdown-toggle').dropdown()
        $(window).resize(handleWindowResize);

        $(window).hashchange( function(){
            removeTm();
            showLoader();
            var cat = getValidCat();
            var topicSpan = $("span.topic-name");
            var menu = $('ul.nav > li.dropdown');
            var menuItem = menu.find('a[href="#' + cat+ '"]')
            topicSpan.text('Topic: ' + menuItem.text())
            sendAjax(cat);
        })

        bindSettingsForm();
        // Trigger the event (useful on page load).
    };

    function disableOrEnableAutoRefresh(){
        if(!state.autorefresh && state.autoRefreshTimer != undefined){
            log("Auto-refresh: off");
            log(arguments)
            clearInterval(state.autoRefreshTimer);
            state.autoRefreshTimer = undefined;
            $(this).attr('checked', false);
            //event.stopPropagation();
        }else if(state.autorefresh && state.autoRefreshTimer == undefined) {
            log("Auto-refresh: on");
            log(arguments)
            state.autoRefreshTimer = setInterval(triggerHashChange, 1000 * 30)
            $(this).attr('checked', true);
            //event.stopPropagation();
        }
    }

    function loadValueIntoFormAndPrepareAutoRefresh(){
        var form = $("#settingsModal form");
        form.find("#autorefresh").prop('checked', state.autorefresh);
        form.find("#user").val(state.screenName);
        disableOrEnableAutoRefresh();
    }

    function bindSettingsForm(){
        var win = $("#settingsModal"),
            form = win.find("form"),
            xhr;

        form.submit(function(){
            var autorefresh=form.find("#autorefresh").is(':checked'),
                screenName=form.find("#user").val();
            $.post(
                'settings.json',
                {
                    "screenName": screenName,
                    "autorefresh": autorefresh
                }
            );
            state.autorefresh = autorefresh;
            state.screenName = screenName;
            disableOrEnableAutoRefresh();
            showLogoutMenu(screenName);
            win.modal('hide')
            return false;
        });
        form.find('#user').typeahead({
            source: function(query, callback){
                if(xhr){
                    xhr.abort();
                }
                xhr = $.ajax({
                    url: '/userTypeAhead.json',
                    data: {
                        q: query
                    },
                    success: function(d){
                        xhr = undefined;
                        callback(d.options);
                    }
                });
            }
        });
        $(".typeahead").css("z-index", 2000);
        checkStatus();
    }

    $(document).ready(onReady);

    function showLogonMenu(){
        $("#screenName").replaceWith("Not logged");
        $(".user-logged").hide();
        $(".user-not-logged").show()
    }

    function showLogoutMenu(userCtx){
        $("#screenName").replaceWith(userCtx);
        $(".user-logged").show();
        $(".user-not-logged").hide()
    }

    function updateTooltip(proc, phase){
        proc = Math.floor(proc);
        $('#fat-menu').tooltip("destroy");
        $('#fat-menu').tooltip({placement: "left", html: true, title: "Done: " + proc + "% (phase: " + phase + "/4)"});
    }

    function checkStatus(){
        $.ajax({
            url: '/settings.json',
            success: function( json ) {
                if(json.is){
                    var settings = json.settings,
                        screenName = settings ? settings.screenName : user;
                    showLogoutMenu(screenName);
                    state.screenName = settings.screenName;
                    state.autorefresh = settings.autorefresh === true || settings.autorefresh === "true";
                    loadValueIntoFormAndPrepareAutoRefresh();

                }else{
                    showLogonMenu();
                    $('#fat-menu').tooltip("destroy");
                }
                triggerHashChange();
            }
        });
    }

})();
