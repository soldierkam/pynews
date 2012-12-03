
TM.Squarified.implement({
    'setColor': function(json) {
      return json.data.$color;
    }
});

(function(){

    var state = {
        loaded: false,
        resizeTimerId: undefined
    }

    function log(str){
        if (typeof window.console !== 'undefined' && typeof window.console.log === 'function'){
            window.console.log(str);
        }
    }

    function showLoader(){
        log("Show loader")
        $('#floatingBarsG').css('display', '');
    }

    function hideLoader(){
        log("Hide loader")
        $('#floatingBarsG').css('display', 'none');
    }

    function showTreeMap(data){

        var tm = new TM.Squarified({
            //Where to inject the treemap.
            rootId: 'infovis',

            //Add click handlers for
            //zooming the Treemap in and out
            addLeftClickHandler: false,
            addRightClickHandler: false,

            //When hovering a node highlight the nodes
            //between the root node and the hovered node. This
            //is done by adding the 'in-path' CSS class to each node.
            selectPathOnHover: true,

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
              onClick: function(tip, node, isLeaf, domElement) {
                  log(node)
              },
              onDbClick: function(tip, node, isLeaf, domElement) {
                  if(node.href){
                    window.open(node.href)
                  } else {
                    log("Missing href");
                    log(node)
                  }
              },
              //implement the onShow method to
              //add content to the tooltip when a node
              //is hovered
              onShow: function(tip, node, isLeaf, domElement) {

                  tip.innerHTML = //"<div class=\"tip-title\">" + node.title + "</div>" +
                    "<div class=\"tip-text\">" + this.makeHTMLFromData(node) + "</div>";
              },

              //Build the tooltip inner html by taking each node data property
              makeHTMLFromData: function(node){
                  var html = '', data = node.data;
                  if ("color" in data){
                      html += "rank" + ': ' + data.$color + '<br />';
                  }
                  if ("image" in data){
                      html += "<img class=\"album\" src=\"" + data.image + "\" />";
                  }
                  if ("lead" in node){
                      html += node.lead;
                  }
                  return html;
              }
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

        function fixTitle(node){
             var titleParts = node.title.split('-'), title;
             if(titleParts.length > 1){
                titleParts.pop();
             }
             node.title = titleParts.join('-').trim();
             $.each(node.children, function(i, node){
                 fixTitle(node);
             });
        }

        function reloadTreeMap(){
            //fixTitle(data);
            //load JSON and plot
            tm.loadJSON(data);
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
            $( '.leaf' ).each(resizeBox);
        };

        $(window).resize(function() {
            if(state.loaded){
                if(state.resizeTimerId){
                    clearTimeout(state.resizeTimerId)
                }else{
                    prepareLoad();
                }
                log("Window resize")
                state.resizeTimerId = setTimeout(function (){
                    log("Resizing...")
                    state.resizeTimerId = undefined;
                    reloadTreeMap();
                }, 200)
            }else{
                log("Ignore resize: not loaded")
            }
        });
        reloadTreeMap();
    }

    var validCategories = {
        'SPORT': 1,
        'POLITICS': 1,
        'SCIENCE': 1,
        'TECHNOLOGY': 1,
        'BUSINESS': 1,
        'HEALTH': 1,
        'WORLD': 1,
        'NATION': 1,
        ELECTIONS: 1,
        HEADLINES: 1,
        //SPOTLIGHT: 1,
        'ENTERTAINMENT': 1,
        'ALL': 1
    };

    function prepareLoad(){
        var hash = window.location.hash.substring(1);
        var cat = 'SPORT';
        if(hash in validCategories){
            cat = hash;
        }else{
            window.location.hash = cat;
            return;
        }
        $('#infovis > div').remove();
        showLoader();
        return cat;
    }

    var type2color={
        "WORLD": "#555555",
        "BUSINESS": "#558855",
        "NATION": "#555588",
        "TECHNOLOGY": "#558888",
        "SCIENCE": "#885555",
        "ELECTIONS": "#888855",
        "POLITICS": "#885588",
        "ENTERTAINMENT": "#888888",
        "SPORT": "#228855",
        "HEALTH": "#225588",
        "HEADLINES": "#882255",
        "SPOTLIGHT": "#558822"
    }

    function findColor(node, parent){
        if(!parent){
            return "#555555";
        }
        var type = parent.title;
        return type2color[type] || "#DDDDDD";
    };

    function createLead(node){
        if(!node.text){
            return node.title;
        }
        var t = node.text.substr(0, 500);
        if(t.length == 500){
            t += "...";
        }
        return t;
    }

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
            mark += r * 100 * tweet.user.followers_count
        })
        return mark;
    };

    var id = 0;

    function prepareData(node, parent){
        var resultNode = {
            children: [],
            data:{
                "$area": getArea(node),
                "$color": findColor(node, parent),
                "href": node.href,
                "tweets": node.tweets
            },
            id: "id-" + id,
            name: node.title,
            lead: createLead(node)
        };
        id += 1;
        $(node.children).each(function(i, child){
            var childResult = prepareData(child, node);
            resultNode.children.push(childResult);
        });
        calcArea(resultNode)
        dropSmallArea(resultNode);
        return resultNode;
    }

    function calcArea(node){
        var area = node.data["$area"];
        $(node.children).each(function(i, child){
            area += child.data["$area"];
        });
        node.data["$area"] = area;
    }

    function dropSmallArea(node){
        var toDelete = []
        var area = 0;
        $(node.children).each(function(i, child){
            var parentArea = node.data["$area"];
            var thisArea = child.data["$area"];
            var ratio = thisArea / parentArea;
            if( ratio < 0.0005){
                log("Ignore " + child.data.href + ": " + ratio)
                log(child.data.tweets)
                toDelete = toDelete.concat(child)
            }else{
                dropSmallArea(child)
            }
        });
        $(toDelete).each(function(i, itemtoRemove){
            node.children.splice($.inArray(itemtoRemove, node.children),1);
        });
    }

    function sendAjax(cat){
        state.loaded = false;
        log("Load " + cat);
        $.ajax({
          url: '/newsmap.json',
          data: {
            c: cat
          },
          success: function( json ) {
              var json = prepareData(json.data)
              showTreeMap(json);
          }
        });
    }

    $(document).ready(function(){
        $('.dropdown-toggle').dropdown()

        $(window).hashchange( function(){
            var cat = prepareLoad();
            var topicSpan = $("span.topic-name");
            var menu = $('ul.nav > li.dropdown');
            var menuItem = menu.find('a[href="#' + cat+ '"]')
            topicSpan.text('Topic: ' + menuItem.text())
            sendAjax(cat);
        })

        // Trigger the event (useful on page load).
        $(window).hashchange();
    });

})();
